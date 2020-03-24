module Logging

open System

type LogLevel =
| Error = 3
| Warning = 2
| Information = 1
| Debug = 0

let LevelToInt (level:LogLevel) = int level

type LogMessage = {level:LogLevel; time:DateTime; text: string}

let logMsg (level:LogLevel) (text:string) = {level = level; time = DateTime.Now ; text = text} 

let logLine msg = sprintf  "[%A][%A] %s" msg.level msg.time msg.text


