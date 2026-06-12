using System;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SistemP_Projekat
{
    class Program
    {
        private static readonly BlockingCollection<HttpListenerContext> _requestQueue = new BlockingCollection<HttpListenerContext>();
        
        private static readonly int _maxConcurrency = 20;
        private static readonly SemaphoreSlim _processingSemaphore = new SemaphoreSlim(_maxConcurrency, _maxConcurrency);
        
        private static readonly CancellationTokenSource _cts = new CancellationTokenSource();

        static async Task Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;

            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:5050/");
            listener.Start();
            Console.WriteLine("Server pokrenut na http://localhost:5050/");

            // GRACEFUL SHUTDOWN: Presretanje Ctrl+C
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true; 
                Console.WriteLine("\n[Sistem] Iniciran graceful shutdown. Zaustavljam prijem novih zahteva...");
                
                _cts.Cancel(); 
                listener.Stop(); 
                _requestQueue.CompleteAdding(); 
            };

            // Pokrećemo pozadinski zadatak iz CacheManager-a
            _ = Task.Factory.StartNew(() => CacheManager.CistiIstekaoKesAsync(_cts.Token), TaskCreationOptions.LongRunning).Unwrap();
            
            // Dispatcher za raspoređivanje zahteva
            _ = Task.Run(() => RasporediZahteve(_cts.Token));

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    HttpListenerContext context = await listener.GetContextAsync();
                    _requestQueue.Add(context);
                }
            }
            catch (HttpListenerException)
            {
                Console.WriteLine("[Main] Listener uspešno zaustavljen.");
            }
            catch (InvalidOperationException)
            {
                // Ignorišemo, dešava se ako se uradi Add() nakon CompleteAdding()
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Main] Greška na listeneru: {ex.Message}");
            }

            Console.WriteLine("[Sistem] Čekam da se tekući zahtevi završe...");
            for (int i = 0; i < _maxConcurrency; i++)
            {
                await _processingSemaphore.WaitAsync();
            }
            
            Console.WriteLine("[Sistem] Svi zahtevi obrađeni. Server je bezbedno ugašen.");
        }

        static void RasporediZahteve(CancellationToken token)
        {
            try
            {
                foreach (var context in _requestQueue.GetConsumingEnumerable(token))
                {
                    _processingSemaphore.Wait(token);

                    // Pozivamo odvojenu biznis logiku iz RequestHandler-a
                    RequestHandler.ObradiZahtevAsync(context)
                        .ContinueWith(t =>
                        {
                            _processingSemaphore.Release();
                            if (t.IsFaulted)
                            {
                                Console.WriteLine($"[Sistem] Kritična greška: {t.Exception?.Flatten().InnerException?.Message}");
                            }
                        }, TaskContinuationOptions.ExecuteSynchronously);
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[Dispatcher] Red za zahteve je uspešno ugašen preko tokena.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Dispatcher] Greška pri raspoređivanju: {ex.Message}");
            }
        }
    }
}