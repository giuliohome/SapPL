open FSharp.Data
open Logging
open System.Configuration
open System.IO
open System
open SqlLib.SqlDB
open FSharp.Control

[<Literal>]
let sample = @"\\my.server.intranet\Outbound\ZXY_TS0120191029122347"

type PL = 
    CsvProvider< 
        Sample = sample,
        Separators = ";", SkipRows = 1, HasHeaders = false, 
        Schema ="CargoID(string),CurrencyType(string),Currency(string),Amount(decimal)">


type CurrencyGroup = {IsInternal: bool; Currency: string}

let tryToInt s = 
    match System.Int32.TryParse s with
    | true, v -> Some v
    | false, _ -> None

let withCargoID (rows:PL.Row[]) =
    rows
    |> Array.choose(fun r -> 
        tryToInt r.CargoID
        |> Option.bind (fun _ -> Some r)
    )

let aggregatePL (rows1:RowSapPL[]) (rows2:RowSapPL[]) =
    Array.append rows1 rows2
    |> Array.groupBy(fun row -> (row.CargoID, row.Currency, row.IsInternal))
    |> Array.map (fun ((cargo, currency, isinternal),rows3) -> 
                    {CargoID = cargo; Currency = currency; IsInternal = isinternal; 
                     Amount = rows3 |> Array.sumBy (fun r -> r.Amount)}
    )
type ParsedResult = {msg: LogMessage[]; rows: RowSapPL[] } with
    static member (+) (res1:ParsedResult, res2:ParsedResult) = 
        {msg = Array.append res1.msg res2.msg; rows = aggregatePL res1.rows res2.rows }
    static member get_Zero () = { msg = [||]; rows = [||]}

type IntermediateResult = | ParseMessage of LogMessage | ParseOutput of RowSapPL
let EUR = "EUR" 
let USD = "USD" 
let parse (filename:string) : ParsedResult = 
    let msg = [|
        let pl = PL.Load filename
        yield ParseMessage (logMsg LogLevel.Information <| sprintf "file %s opened" filename)
        let rows = pl.Rows |> Seq.toArray
        let header = rows |> Array.head
        yield 
            sprintf 
                "rows #: %d with cargoes %d" 
                rows.Length (withCargoID rows).Length
            |> logMsg LogLevel.Information |> ParseMessage
        yield  
            sprintf 
                "first line => CargoID: %s, CurrencyType: %s, Currency: %s, Amount: %f!" 
                header.CargoID header.CurrencyType header.Currency header.Amount
            |> logMsg LogLevel.Debug |> ParseMessage
        let currGroups = 
            rows
            |> Array.groupBy( fun r ->
                {Currency = r.Currency; IsInternal = r.CurrencyType = "I"} )
    
        for (g, r) in currGroups do
            yield 
                sprintf 
                    "Currency %s Internal %b #: %d with cargoes: %d" 
                    g.Currency g.IsInternal r.Length (withCargoID r).Length
                |> logMsg LogLevel.Debug |> ParseMessage
    
    
        yield currGroups 
            |> Array.sumBy(fun (g, r) -> r.Length)
            |> sprintf "total records in all currencies %d" 
            |> logMsg LogLevel.Information |> ParseMessage 
        yield currGroups
            |> Array.sumBy(fun (g, r) -> (withCargoID r).Length)
            |> sprintf "total with cargoes in all currencies %d"  
            |> logMsg LogLevel.Information |> ParseMessage 
        yield logMsg LogLevel.Information "parsed fine!" |> ParseMessage

        yield!  
            currGroups 
            |> Array.filter(fun (g , _) -> 
                (g.Currency = EUR && g.IsInternal) 
                || (g.Currency = USD && not g.IsInternal) )
            |> Array.collect(fun (_, rows) -> 
                (withCargoID rows) 
                |> Array.map(fun row -> 
                    ParseOutput 
                        {Currency = row.Currency; Amount = row.Amount; 
                        CargoID = int row.CargoID; IsInternal = row.CurrencyType = "I" }))
    |]
    {msg = msg |> Array.choose(function | ParseMessage pm -> Some pm | _ -> None); 
    rows =  msg |> Array.choose(function | ParseOutput pr -> Some pr | _ -> None)}

let sapFileName = "ZIOBAK055101_" // + branch + "*"
let branches = [| "TS01"; "TSSG"; "TSUK" |]
let safeParse (folderPath:string) (fileMatch:string) : ParsedResult  = 
    if not (Directory.Exists(folderPath)) then 
        { msg = [| sprintf "non existing folder dir %s" folderPath
        |> logMsg LogLevel.Error |]; rows = [||]}
    else
        try
            match 
                Directory.EnumerateFiles(folderPath, fileMatch) 
                |> Seq.sortDescending
                |> Seq.tryHead with
            | Some filename -> 
                parse filename
            | None -> 
                { msg = [| folderPath
                |> sprintf "No Sap files in folder: %s" 
                |> logMsg LogLevel.Error |]; rows = [||] }
        with 
        |exc -> 
            { msg = [| exc.Message
            |> sprintf "parse failed: %s" 
            |> logMsg LogLevel.Error |]; rows = [||] }
    

let MainLevel =
#if DEBUG
    LogLevel.Debug
#else 
    LogLevel.Information
#endif 

[<EntryPoint>]
let main argv = 
    let logPath = ConfigurationManager.AppSettings.["LogFolder"]  
    if not (Directory.Exists(logPath)) then 
        printfn "non existing log dir %s" logPath
        -1
    else
        try
            use sw = new StreamWriter(Path.Combine(logPath,"Log_SapPl_" + DateTime.UtcNow.ToString("yyyy_MM_dd") + ".txt"), true)
            let folderPath = ConfigurationManager.AppSettings.["OutboundFolder"]
            let parsed = 
                branches
                |> Array.map(fun branch -> 
                    sapFileName + branch + "*" |> safeParse folderPath)
                |> Array.sum
            [| logMsg LogLevel.Debug "End of Sap File Parsing"; |]
            |> Array.append parsed.msg
            |> Array.append [| logMsg LogLevel.Debug "Start of Sap File Parsing"; |]
            |> Array.filter(fun l -> l.level >= MainLevel )
            |> Array.iter( fun l -> sw.WriteLine (logLine l) )
            sw.Flush()
            if  parsed.rows |> Array.length > 0 then
                let db = DB()
                async {
                    let! load_results = 
                        db.loadSapPL parsed.rows
                        |> AsyncSeq.toListAsync
                    [ logMsg LogLevel.Debug "End of DB Load"; ]
                    |> List.append (
                        load_results
                        |> List.map(function 
                            | Ok (i, s) -> 
                                logMsg LogLevel.Information <| sprintf "Ok cargo %i: %s " i s
                            | Error (m1,m2) -> 
                                logMsg LogLevel.Error <| sprintf "Error %s: %s " m1 m2))
                    |> List.append  [ logMsg LogLevel.Debug "Start of DB Load"; ]
                    |> List.filter(fun l -> l.level >= MainLevel )
                    |> List.iter( fun l -> sw.WriteLine (logLine l) )
                    printfn "done (see the log)"
                } |> Async.RunSynchronously
            0
        with 
        | exc ->
            printfn "%s" exc.Message
            printfn "%s" exc.StackTrace
            -1
