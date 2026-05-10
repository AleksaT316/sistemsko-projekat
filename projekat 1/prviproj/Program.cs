using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;

namespace SistemP_Projekat
{
    // Klasa za stavke u kesu, pamtimo same podatke i tacno vreme kad isticu
    public class CacheItem
    {
        public string Data { get; set; }
        public DateTime ExpirationTime { get; set; }
    }

    class Program
    {
        // 1. Deljeni red za zahteve
        // Ovde obavezno cepamo Monitor.Wait i Pulse jer su tako izricito trazili u PDF-u
        private static readonly Queue<HttpListenerContext> _requestQueue = new Queue<HttpListenerContext>();
        private static readonly object _queueLock = new object();

        // 2. Thread-safe kes 
        // ConcurrentDictionary radi posao vrhunski
        private static readonly ConcurrentDictionary<string, CacheItem> _cache = new ConcurrentDictionary<string, CacheItem>();
        private static readonly TimeSpan _cacheDuration = TimeSpan.FromMinutes(2); 

        // 3. STRIPED LOCKING: Glavna fora za resavanje Cache Stampede problema
        // Pravimo poseban katanac za svaki upit (npr. poseban lock za "harry", poseban za "ai")
        // Ovako ne blokiramo ceo server ako vise ljudi trazi razlicite stvari istovremeno
        private static readonly ConcurrentDictionary<string, object> _queryLocks = new ConcurrentDictionary<string, object>();

        private static readonly HttpClient _httpClient = new HttpClient();

        static void Main(string[] args)
        {
            // Worker niti koje ce da zvacu poslove iz reda
            int brojNiti = 20;
            for (int i = 0; i < brojNiti; i++)
            {
                Thread worker = new Thread(ObradiZahteve);
                worker.Name = $"Worker-{i + 1}";
                worker.Start();
            }

            // Dizemo server
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:5050/");
            listener.Start();
            Console.WriteLine("Server pokrenut na http://localhost:5050/");

            // Pozadinska nit za aktivno ciscenje memorije
            // Cisti djubre iz kesa da ne curi memorija
            Thread cacheCleaner = new Thread(CistiIstekaoKes);
            cacheCleaner.IsBackground = true; // gasi se sama kad ugasim server
            cacheCleaner.Start();

            // Beskonacna petlja gde main nit samo slusa i gura u red
            while (true)
            {
                try
                {
                    HttpListenerContext context = listener.GetContext();
                    
                    lock (_queueLock)
                    {
                        _requestQueue.Enqueue(context);
                        Monitor.Pulse(_queueLock); // cimne prvu slobodnu worker nit da ima posla
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Main] Greska na listeneru: {ex.Message}");
                }
            }
        }

        // Ovo vrte worker niti paralelno
        static void ObradiZahteve()
        {
            while (true)
            {
                HttpListenerContext context;

                // Uzimanje iz reda uz blokirajucu sinhronizaciju
                lock (_queueLock)
                {
                    while (_requestQueue.Count == 0)
                    {
                        Monitor.Wait(_queueLock); // ovde nit bukvalno spava, ne trosi procesor bezveze
                    }
                    context = _requestQueue.Dequeue();
                }

                if (context.Request.HttpMethod != "GET")
                {
                    PosaljiOdgovor(context, "{\"error\":\"Koristite GET metodu\"}", 405);
                    continue;
                }

                string query = context.Request.QueryString["q"];
                if (string.IsNullOrEmpty(query))
                {
                    PosaljiOdgovor(context, "{\"error\":\"Nedostaje 'q' parametar\"}", 400);
                    continue;
                }

                string cacheKey = query.ToLower();
                string rezultat = null;

                // 1. PRVA PROVERA: Da l je mozda vec u kesu i validno? (brzo citanje)
                if (_cache.TryGetValue(cacheKey, out CacheItem cachedItem) && DateTime.Now < cachedItem.ExpirationTime)
                {
                    Console.WriteLine($"[{Thread.CurrentThread.Name}] CACHE HIT -> '{query}'");
                    rezultat = cachedItem.Data;
                }
                else
                {
                    // AKO NIJE U KESU -> idemo na API al zakljucavamo samo za taj jedan pojam

                    // Izvadi mi katanac samo za ovu rec
                    object pojamLock = _queryLocks.GetOrAdd(cacheKey, _ => new object());

                    lock (pojamLock)
                    {
                        // 2. DVOSTRUKA PROVERA (Double-Check Locking)
                        // Dok smo mi cekali u redu za ovaj lock, mozda je neka druga nit vec odradila posao i upisala u kes?
                        if (_cache.TryGetValue(cacheKey, out CacheItem doubleCheckItem) && DateTime.Now < doubleCheckItem.ExpirationTime)
                        {
                            Console.WriteLine($"[{Thread.CurrentThread.Name}] CACHE HIT (Spasen lock-om!) -> '{query}'");
                            rezultat = doubleCheckItem.Data;
                        }
                        else
                        {
                            // 3. Nema ga i dalje, mi smo prva nit koja dovlaci podatke za ovo!
                            Console.WriteLine($"[{Thread.CurrentThread.Name}] CACHE MISS -> '{query}' (API poziv)");
                            string apiUrl = $"https://www.googleapis.com/books/v1/volumes?q={Uri.EscapeDataString(query)}";

                            try
                            {
                                HttpResponseMessage response = _httpClient.GetAsync(apiUrl).Result;
                                
                                if (response.IsSuccessStatusCode)
                                {
                                    rezultat = response.Content.ReadAsStringAsync().Result;
                                    
                                    // Sve dobro, sacuvaj na 2 min
                                    _cache[cacheKey] = new CacheItem { Data = rezultat, ExpirationTime = DateTime.Now.Add(_cacheDuration) };
                                }
                                else
                                {
                                    rezultat = "{\"error\":\"Knjige nisu pronadjene ili nas je google bloknuo (Rate Limit).\"}";
                                    
                                    // NEGATIVE CACHING fora: ako pukne ili nas blokiraju, kesiraj to nakratko (30s)
                                    // da ne bi uleteli u beskonacnu petlju slanja zahteva i skroz pukli
                                    _cache[cacheKey] = new CacheItem { Data = rezultat, ExpirationTime = DateTime.Now.AddSeconds(30) };
                                    Console.WriteLine("Upisana greska u kesu!! " + response.StatusCode);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[{Thread.CurrentThread.Name}] Greska na API-ju: {ex.Message}");
                                rezultat = "{\"error\":\"Greska pri radu sa API-jem.\"}";
                                
                                // Opet negative caching za exception
                                _cache[cacheKey] = new CacheItem { Data = rezultat, ExpirationTime = DateTime.Now.AddSeconds(30) };
                            }
                        }
                    } // ovde se pusta lock za taj specifikacan pojam
                }

                PosaljiOdgovor(context, rezultat, 200);
            }
        }

        static void PosaljiOdgovor(HttpListenerContext context, string sadrzaj, int statusCode)
        {
            // Sredjujemo CORS da bi mogo frontend (JS skripta) da gadja server bez problema
            context.Response.AppendHeader("Access-Control-Allow-Origin", "*");
            context.Response.AppendHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
            context.Response.AppendHeader("Access-Control-Allow-Headers", "Content-Type, Accept");

            // Rucno handlanje OPTIONS zahteva sto browser salje pre pravog zahteva (preflight)
            if (context.Request.HttpMethod == "OPTIONS")
            {
                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.OutputStream.Close();
                return;
            }

            // Pisanje i slanje odgovora
            byte[] buffer = Encoding.UTF8.GetBytes(sadrzaj);
            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/json; charset=utf-8";
            context.Response.ContentLength64 = buffer.Length;
            context.Response.OutputStream.Write(buffer, 0, buffer.Length);
            context.Response.OutputStream.Close();
        }

        // Metoda za aktivno brisanje kesa iz pozadine
        static void CistiIstekaoKes()
        {
            while (true)
            {
                // Spavanje 60 sekundi izmedju ciscenja
                Thread.Sleep(60000); 

                Console.WriteLine("[Cleaner] Pokretanje periodicnog ciscenja kesa...");

                foreach (var kvp in _cache)
                {
                    if (DateTime.Now > kvp.Value.ExpirationTime)
                    {
                        // Brisi samo ako je vreme isteklo
                        if (_cache.TryRemove(kvp.Key, out _))
                        {
                            Console.WriteLine($"[Cleaner] Izbacen istekao kljuc: '{kvp.Key}'");                  
                            
                            // Obrisi obavezno i lock objekat za taj kljuc da ne bi i to jelo memoriju vremenom
                            _queryLocks.TryRemove(kvp.Key, out _);
                        }
                    }
                }
            }
        }
    }
}