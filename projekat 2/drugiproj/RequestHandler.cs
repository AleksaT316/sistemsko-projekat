using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SistemP_Projekat
{
    public static class RequestHandler
    {
        private static readonly HttpClient _httpClient = new HttpClient();

        public static async Task ObradiZahtevAsync(HttpListenerContext context)
        {
            int threadId = Environment.CurrentManagedThreadId;

            if (context.Request.HttpMethod != "GET")
            {
                PosaljiOdgovor(context, "{\"error\":\"Metoda nije dozvoljena\"}", 405);
                return;
            }

            string query = context.Request.QueryString["q"];
            if (string.IsNullOrEmpty(query))
            {
                PosaljiOdgovor(context, "{\"error\":\"Nedostaje 'q' parametar\"}", 400);
                return;
            }

            string cacheKey = query.ToLower();
            string rezultat = null;
            int responseStatusCode = 200;

            // 1. Provera u CacheManager-u
            if (CacheManager.Cache.TryGetValue(cacheKey, out CacheItem cachedItem) && DateTime.Now < cachedItem.ExpirationTime)
            {
                Console.WriteLine($"[Nit-{threadId}] CACHE HIT -> '{query}'");
                rezultat = cachedItem.Data;
            }
            else
            {
                object pojamLock = CacheManager.QueryLocks.GetOrAdd(cacheKey, _ => new object());

                lock (pojamLock)
                {
                    if (CacheManager.Cache.TryGetValue(cacheKey, out CacheItem doubleCheckItem) && DateTime.Now < doubleCheckItem.ExpirationTime)
                    {
                        rezultat = doubleCheckItem.Data;
                    }
                    else
                    {
                        Console.WriteLine($"[Nit-{threadId}] CACHE MISS -> '{query}'");
                        string apiUrl = $"https://www.googleapis.com/books/v1/volumes?q={Uri.EscapeDataString(query)}";

                        try
                        {
                            HttpResponseMessage response = _httpClient.GetAsync(apiUrl).GetAwaiter().GetResult();

                            if (response.IsSuccessStatusCode)
                            {
                                rezultat = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                                CacheManager.Cache[cacheKey] = new CacheItem { Data = rezultat, ExpirationTime = DateTime.Now.Add(CacheManager.Duration) };
                            }
                            else
                            {
                                responseStatusCode = (int)response.StatusCode;
                                
                                if (responseStatusCode == 429)
                                    rezultat = "{\"error\":\"Google API Rate Limit (Too Many Requests).\"}";
                                else if (responseStatusCode == 404)
                                    rezultat = "{\"error\":\"Knjige nisu pronađene (Not Found).\"}";
                                else
                                    rezultat = $"{{\"error\":\"API Greška: {response.StatusCode}\"}}";

                                CacheManager.Cache[cacheKey] = new CacheItem { Data = rezultat, ExpirationTime = DateTime.Now.AddSeconds(30) };
                            }
                        }
                        catch (HttpRequestException netEx)
                        {
                            Console.WriteLine($"[Nit-{threadId}] Mrežna greška: {netEx.Message}");
                            responseStatusCode = 502; // Bad Gateway
                            rezultat = "{\"error\":\"Nemoguće uspostaviti vezu sa Google API-jem.\"}";
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Nit-{threadId}] Interna greška: {ex.Message}");
                            responseStatusCode = 500; // Internal Server Error
                            rezultat = "{\"error\":\"Došlo je do greške na serveru.\"}";
                        }
                    }
                }
            }

            PosaljiOdgovor(context, rezultat, responseStatusCode);
        }

        private static void PosaljiOdgovor(HttpListenerContext context, string sadrzaj, int statusCode)
        {
            try
            {
                context.Response.AppendHeader("Access-Control-Allow-Origin", "*");
                context.Response.AppendHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
                context.Response.AppendHeader("Access-Control-Allow-Headers", "Content-Type, Accept");

                if (context.Request.HttpMethod == "OPTIONS")
                {
                    context.Response.StatusCode = 200;
                    context.Response.OutputStream.Close();
                    return;
                }

                byte[] buffer = Encoding.UTF8.GetBytes(sadrzaj ?? "{}");
                
                context.Response.StatusCode = statusCode;
                context.Response.ContentType = "application/json; charset=utf-8";
                context.Response.ContentLength64 = buffer.Length;
                context.Response.OutputStream.Write(buffer, 0, buffer.Length);
                context.Response.OutputStream.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Odgovor] Greška prilikom slanja: {ex.Message}");
            }
        }
    }
}