using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace SistemP_Projekat
{
    public static class CacheManager
    {
        // Deljene strukture prebačene ovde
        public static readonly ConcurrentDictionary<string, CacheItem> Cache = new ConcurrentDictionary<string, CacheItem>();
        public static readonly ConcurrentDictionary<string, object> QueryLocks = new ConcurrentDictionary<string, object>();
        public static readonly TimeSpan Duration = TimeSpan.FromMinutes(2);

        // Metoda za aktivno brisanje keša iz pozadine
        public static async Task CistiIstekaoKesAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(60000, token);
                    
                    Console.WriteLine("[Čistač] Pokretanje periodičnog čišćenja keša...");
                    foreach (var kvp in Cache)
                    {
                        if (DateTime.Now > kvp.Value.ExpirationTime)
                        {
                            if (Cache.TryRemove(kvp.Key, out _))
                            {
                                Console.WriteLine($"[Čistač] Izbačen istekao ključ: '{kvp.Key}'");
                                QueryLocks.TryRemove(kvp.Key, out _);
                            }
                        }
                    }
                }
                catch (TaskCanceledException)
                {
                    Console.WriteLine("[Čistač] Pozadinski zadatak zaustavljen.");
                    break;
                }
            }
        }
    }
}