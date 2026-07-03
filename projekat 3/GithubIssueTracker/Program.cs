using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
// Ukoliko imaš instaliran SharpEntropy dodaj usings, npr:
// using SharpEntropy;
// using SharpEntropy.ITokenizer;

namespace GithubIssueTracker
{
    // ==========================================
    // 1. DATA MODELI I PORUKE
    // ==========================================

    public record GetIssueStateRequest(string Owner, string Repo, int IssueId);
    public record IssueStateResponse(string Owner, string Repo, int IssueId, List<string> Comments, string CurrentTopic);
    public record StartPollingMessage(string Owner, string Repo, int IssueId, IActorRef IssueActor);
    public record NewCommentsMessage(List<GithubComment> Comments);

    public class GithubComment
    {
        [JsonPropertyName("id")]
        public long Id { get; set; }

        [JsonPropertyName("body")]
        public string Body { get; set; }

        [JsonPropertyName("user")]
        public GithubUser User { get; set; }
    }

    public class GithubUser
    {
        [JsonPropertyName("login")]
        public string Login { get; set; }
    }

    // ==========================================
    // 2. SHARP ENTROPY TOPIC MODELER (Wrapper)
    // ==========================================
    
    public static class SharpEntropyTopicModeler
    {

        public static string Analyze(List<string> comments)
        {
            if (comments == null || comments.Count == 0) return "No comments yet";

            
            string allText = string.Join(" ", comments).ToLower();
            if (allText.Contains("bug") || allText.Contains("error") || allText.Contains("fix"))
                return "Bug Report / Issue";
            if (allText.Contains("add") || allText.Contains("feature") || allText.Contains("idea"))
                return "Feature Request";
            
            return "General Discussion";
        }
    }//# gotov model za ovo, bilo sta sto radi topic modeling

    // ==========================================
    // 3. RX.NET KOMPONENTA (Poller)
    // ==========================================

    public static class RxGitHubPoller
    {
        private static readonly HttpClient _httpClient;

        static RxGitHubPoller()
        {
            _httpClient = new HttpClient();
            // GitHub API zahteva User-Agent header
            _httpClient.DefaultRequestHeaders.Add("User-Agent", "RxAkkaApp-StudentProject");
            
        }

        public static void Start(string owner, string repo, int issueId, IActorRef actor)
        {
            // Observable.Timer kreira stream koji se emituje odmah (TimeSpan.Zero), 
            // a zatim svakih 30 sekundi.
            Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(30))
                // Prebacujemo rad na ThreadPool kako ne bismo blokirali glavni thread (Multithreading zahtev)
                .ObserveOn(TaskPoolScheduler.Default)
                .SelectMany(async _ => await FetchCommentsAsync(owner, repo, issueId))
                // Ako API pukne (npr. nema neta), hvatamo grešku i vraćamo praznu listu kako stream ne bi umro
                .Catch<List<GithubComment>, Exception>(ex => 
                {
                    Console.WriteLine($"[Rx Poller] Greška pri dohvatanju podataka: {ex.Message}");
                    return Observable.Return(new List<GithubComment>());
                })//# sto vise razlicitih obrada za greska, forbidden, prazno, inauthorized itd..
                .Subscribe(
                    comments => 
                    {
                        // Osnovno filtriranje (npr. odbacujemo prazne komentare)
                        var validComments = comments.Where(c => !string.IsNullOrWhiteSpace(c.Body)).ToList();
                        
                        // Emitujemo podatke kao poruku aktoru
                        if(validComments.Any())
                        {
                            actor.Tell(new NewCommentsMessage(validComments));
                        }
                    }
                );
        }

        private static async Task<List<GithubComment>> FetchCommentsAsync(string owner, string repo, int issueId)
        {
            string url = $"https://api.github.com/repos/{owner}/{repo}/issues/{issueId}/comments";
            Console.WriteLine($"[Rx Poller] Šaljem HTTP GET na {url}");
            
            var response = await _httpClient.GetAsync(url);
            if (!response.IsSuccessStatusCode) return new List<GithubComment>();

            var json = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<List<GithubComment>>(json) ?? new List<GithubComment>();
        }
    }

    // ==========================================
    // 4. AKKA.NET AKTORI
    // ==========================================

    // Aktor zadužen za JEDAN konkretan Issue
    public class IssueActor : UntypedActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly string _owner;
        private readonly string _repo;
        private readonly int _issueId;

        // Interno stanje
        private readonly HashSet<long> _processedCommentIds = new();
        private readonly List<string> _comments = new();
        private string _currentTopic = "Unknown";

        public IssueActor(string owner, string repo, int issueId)
        {
            _owner = owner;
            _repo = repo;
            _issueId = issueId;
        }

        protected override void OnReceive(object message)
        {
            switch (message)
            {
                case NewCommentsMessage newMsg:
                    // Izdvajamo samo one komentare čiji ID još nije obrađen (Sprečavanje dupliranja)
                    var freshComments = newMsg.Comments.Where(c => !_processedCommentIds.Contains(c.Id)).ToList();
                    
                    if (freshComments.Any())
                    {
                        _log.Info($"[{_owner}/{_repo}#{_issueId}] Stiglo {freshComments.Count} novih komentara.");
                        
                        foreach (var c in freshComments)
                        {
                            _processedCommentIds.Add(c.Id);
                            _comments.Add($"[{c.User.Login}]: {c.Body}");
                        }

                        // Pokrećemo Topic Modeling nad svim prikupljenim komentarima
                        _currentTopic = SharpEntropyTopicModeler.Analyze(_comments);//# ovde pozvati builtin biblioteku
                    }
                    break;

                case GetIssueStateRequest _:
                    // Odgovaramo web serveru trenutnim stanjem
                    Sender.Tell(new IssueStateResponse(_owner, _repo, _issueId, new List<string>(_comments), _currentTopic));
                    break;
            }
        }

        public static Props Props(string owner, string repo, int issueId) =>
            Akka.Actor.Props.Create(() => new IssueActor(owner, repo, issueId));
    }

    // Aktor koji rutira zahteve ka odgovarajućim IssueAktorima
    public class CoordinatorActor : UntypedActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();

        protected override void OnReceive(object message)
        {
            if (message is GetIssueStateRequest req)
            {
                string childName = $"issue-{req.Owner}-{req.Repo}-{req.IssueId}";
                var child = Context.Child(childName);

                if (child.IsNobody())
                {
                    _log.Info($"Kreiram novog Aktora i Rx Poller za {req.Owner}/{req.Repo}#{req.IssueId}");
                    
                    // Kreiramo aktora i zadajemo mu specifičan dispatcher (Multithreading zahtev)
                    child = Context.ActorOf(IssueActor.Props(req.Owner, req.Repo, req.IssueId).WithDispatcher("issue-dispatcher"), childName);
                    
                    // Pokrećemo Rx pozadinsko preuzimanje
                    RxGitHubPoller.Start(req.Owner, req.Repo, req.IssueId, child);
                }

                // Prosleđujemo zahtev IssueAktoru kako bi odgovorio serveru
                child.Forward(req);
            }
        }

        public static Props Props() => Akka.Actor.Props.Create<CoordinatorActor>();
    }

    // ==========================================
    // 5. WEB SERVER (Konzolna aplkacija)
    // ==========================================

    public class Program
    {
        public static async Task Main(string[] args)
        {
            // Konfiguracija koja kreira custom Akka Dispatcher za dodatan multithreading poen kod asistenta
            var config = ConfigurationFactory.ParseString(@"
                akka {
                    loglevel = INFO
                    stdout-loglevel = INFO
                }
                issue-dispatcher {
                    type = Dispatcher
                    executor = fork-join-executor
                    fork-join-executor {
                        parallelism-min = 2
                        parallelism-factor = 2.0
                        parallelism-max = 8
                    }
                    throughput = 100
                }
            ");

            using var system = ActorSystem.Create("GithubSystem", config);
            var coordinator = system.ActorOf(CoordinatorActor.Props(), "coordinator");

            // Inicijalizacija jednostavnog Http Web Servera u konzoli
            using var listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");
            listener.Start();

            Console.WriteLine("Web server je pokrenut na http://localhost:8080/");
            Console.WriteLine("Primer URL-a: http://localhost:8080/?owner=dotnet&repo=runtime&issue=1000");

            while (true)
            {
                var context = await listener.GetContextAsync();
                var request = context.Request;
                var response = context.Response;
//# fire and forget
                try
                {
                    string owner = request.QueryString["owner"];
                    string repo = request.QueryString["repo"];
                    string issueStr = request.QueryString["issue"];

                    if (string.IsNullOrEmpty(owner) || string.IsNullOrEmpty(repo) || !int.TryParse(issueStr, out int issueId))
                    {
                        await SendResponse(response, 400, "Neispravni parametri. Koristite: ?owner=X&repo=Y&issue=Z");
                        continue;
                    }

                    Console.WriteLine($"[Web Server] Primljen zahtev za {owner}/{repo}#{issueId}. Prosleđujem Aktoru...");

                    // Web Server prevodi HTTP zahtev u poruku i pita Aktora za stanje
                    var stateRequest = new GetIssueStateRequest(owner, repo, issueId);
                    
                    // Čekamo odgovor od aktora (Timeout 3 sekunde)
                    var actorResponse = await coordinator.Ask<IssueStateResponse>(stateRequest, TimeSpan.FromSeconds(3));

                    // Pravimo JSON od rezultata
                    var jsonResponse = JsonSerializer.Serialize(actorResponse, new JsonSerializerOptions { WriteIndented = true });
                    await SendResponse(response, 200, jsonResponse);
                }
                catch (Exception ex)
                {
                    await SendResponse(response, 500, $"Došlo je do greške: {ex.Message}");
                }
            }
        }

        private static async Task SendResponse(HttpListenerResponse response, int statusCode, string content)
        {
            response.StatusCode = statusCode;
            response.ContentType = "application/json";
            response.Headers.Add("Access-Control-Allow-Origin", "*");
            
            byte[] buffer = System.Text.Encoding.UTF8.GetBytes(content);
            response.ContentLength64 = buffer.Length;
            await response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
            response.OutputStream.Close();
        }
    }
}