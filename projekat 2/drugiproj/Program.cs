using System;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Channels; // Obavezno dodaj ovaj namespace
using System.Threading.Tasks;

namespace SistemP_Projekat
{
    class Program
    {
        // 1. ZAMENA: Umesto BlockingCollection, pravimo Unbounded (neograničeni) asinhroni kanal
        private static readonly Channel<HttpListenerContext> _requestChannel = Channel.CreateUnbounded<HttpListenerContext>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false }
        );
        
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
                _requestChannel.Writer.Complete(); // Označavamo kanalu da više nema upisa
            };

            _ = Task.Factory.StartNew(() => CacheManager.CistiIstekaoKesAsync(_cts.Token), TaskCreationOptions.LongRunning).Unwrap();
            
            // Asinhroni Dispatcher za raspoređivanje zahteva
            _ = Task.Run(() => RasporediZahteveAsync(_cts.Token));

            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    HttpListenerContext context = await listener.GetContextAsync();
                    
                    // 2. ASINHRONI UPIS U KANAL: Umesto .Add() koristimo TryWrite ili WriteAsync
                    _requestChannel.Writer.TryWrite(context);
                }
            }
            catch (HttpListenerException)
            {
                Console.WriteLine("[Main] Listener uspešno zaustavljen.");
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

        // 3. ASINHRONI DISPATCHER (Potpuno neblokirajući rad)
        static async Task RasporediZahteveAsync(CancellationToken token)
        {
            try
            {
                // WaitToReadAsync asinhrono čeka da se pojavi nešto u kanalu (0% CPU, 0 blokiranih niti)
                while (await _requestChannel.Reader.WaitToReadAsync(token))
                {
                    // Izvlačimo sve dostupne zahteve iz kanala
                    while (_requestChannel.Reader.TryRead(out var context))
                    {
                        // Čekamo dozvolu semafora asinhrono pre nego što pustimo task u rad
                        await _processingSemaphore.WaitAsync(token);

                        _ = RequestHandler.ObradiZahtevAsync(context)
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
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("[Dispatcher] Kanal za zahteve je uspešno ugašen preko tokena.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Dispatcher] Greška pri raspoređivanju: {ex.Message}");
            }
        }
    }
}