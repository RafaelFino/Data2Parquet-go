package logger

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type LogLevel int

const (
	LevelError LogLevel = iota
	LevelWarn
	LevelInfo
	LevelDebug
)

type FormatterType int

const (
	FormatterColorText FormatterType = iota
	FormatterText
	FormatterJson
	FormatterMultiLineColor
)

var FormatterMap = map[string]FormatterType{
	"color": FormatterColorText,
	"text":  FormatterText,
	"json":  FormatterJson,
	"multi": FormatterMultiLineColor,
}

var logLevelMap = map[LogLevel]string{
	LevelError: "ERR",
	LevelWarn:  "WAR",
	LevelInfo:  "INF",
	LevelDebug: "DEB",
}

type LogItem struct {
	Level LogLevel          `json:"level"`
	Msg   string            `json:"msg"`
	Args  map[string]string `json:"args"`
	When  time.Time         `json:"when"`
}

var Reset = "\033[0m"
var Red = "\033[31m"
var Green = "\033[32m"
var Yellow = "\033[33m"
var Blue = "\033[34m"
var Magenta = "\033[35m"
var Cyan = "\033[36m"
var Gray = "\033[37m"
var White = "\033[97m"

type Logger struct {
	DefaultLevel LogLevel
	data         chan *LogItem
	formatter    func(item *LogItem) string
}

var instance = NewLogger()

func GetLogger() *Logger {
	return instance
}

func NewLogger() *Logger {
	ret := &Logger{
		data:         make(chan *LogItem, 1024*4),
		DefaultLevel: LevelInfo,
		formatter:    colorTextFormatter,
	}

	go ret.writer()

	return ret
}

func (l *Logger) SetLogLoggerLevel(level LogLevel) {
	l.DefaultLevel = level
}

func (l *Logger) GetLogLoggerLevel() LogLevel {
	return l.DefaultLevel
}

func (l *Logger) SetFormatterByName(formatter string) {
	if f, ok := FormatterMap[strings.ToLower(formatter)]; ok {
		l.SetFormater(f)
	} else {
		l.SetFormater(FormatterColorText)
	}
}

func (l *Logger) SetFormater(formatter FormatterType) {
	switch formatter {
	case FormatterColorText:
		l.formatter = colorTextFormatter
	case FormatterText:
		l.formatter = textFormatter
	case FormatterJson:
		l.formatter = jsonFormatter
	case FormatterMultiLineColor:
		l.formatter = multiLineColorFormatter
	default:
		l.formatter = colorTextFormatter
	}
}

func (l *Logger) getArgs(args []any) map[string]string {
	ret := make(map[string]string)

	for len(args)%2 != 0 {
		args = append(args, "MISSING")
	}

	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			key := fmt.Sprintf("%v", args[i])
			value := fmt.Sprintf("%v", args[i+1])
			ret[key] = value
		}
	}

	return ret
}

func (l *Logger) Debug(msg string, args ...any) {
	if l.DefaultLevel == LevelDebug {
		l.data <- &LogItem{
			Level: LevelDebug,
			When:  time.Now(),
			Msg:   msg,
			Args:  l.getArgs(args),
		}
	}
}
func (l *Logger) Info(msg string, args ...interface{}) {
	l.data <- &LogItem{
		Level: LevelInfo,
		When:  time.Now(),
		Msg:   msg,
		Args:  l.getArgs(args),
	}
}

func (l *Logger) Warn(msg string, args ...interface{}) {
	l.data <- &LogItem{
		Level: LevelWarn,
		When:  time.Now(),
		Msg:   msg,
		Args:  l.getArgs(args),
	}

}
func (l *Logger) Error(msg string, args ...interface{}) {
	l.data <- &LogItem{
		Level: LevelError,
		When:  time.Now(),
		Msg:   msg,
		Args:  l.getArgs(args),
	}
}

func (l *Logger) Close() {
	close(l.data)
}

func (l *Logger) writer() {
	running := true

	for running {
		item, running := <-l.data

		if running && item != nil {
			fmt.Println(l.formatter(item))
		}
	}
}

func colorTextFormatter(item *LogItem) string {
	return fmt.Sprintf("%s%s %s %s%s%s   %s%s", Blue, item.When.Format("2006-01-02 15:04:05"), getColorLevel(item.Level), White, item.Msg, Reset, colorPrintArgs(item.Args), Reset)
}

func textFormatter(item *LogItem) string {
	return fmt.Sprintf("%s %s %s\t%s", item.When.Format("2006-01-02 15:04:05"), getLevel(item.Level), item.Msg, printArgs(item.Args))
}

func jsonFormatter(item *LogItem) string {
	ret, err := json.Marshal(item)

	if err != nil {
		return fmt.Sprintf("{\"when\":\"%s\",\"level\":\"%s\",\"msg\":\"%s\",\"args\":%v}", item.When.Format("2006-01-02 15:04:05"), getLevel(item.Level), item.Msg, item.Args)
	}

	return string(ret)
}

func multiLineColorFormatter(item *LogItem) string {
	return fmt.Sprintf("%s%s %s %s%s%s%s%s", Blue, item.When.Format("2006-01-02 15:04:05"), getColorLevel(item.Level), White, item.Msg, Reset, colorMultilinePrintArgs(item.Args), Reset)
}

func getColorLevel(level LogLevel) string {
	switch level {
	case LevelError:
		return fmt.Sprintf("%s%s%s", Red, logLevelMap[level], Reset)
	case LevelWarn:
		return fmt.Sprintf("%s%s%s", Yellow, logLevelMap[level], Reset)
	case LevelInfo:
		return fmt.Sprintf("%s%s%s", Green, logLevelMap[level], Reset)
	case LevelDebug:
		return fmt.Sprintf("%s%s%s", Gray, logLevelMap[level], Reset)
	}

	return "UNK"
}

func getLevel(level LogLevel) string {
	return logLevelMap[level]
}

func colorPrintArgs(args map[string]string) string {
	ret := ""
	for k, v := range args {
		ret += fmt.Sprintf("  %s%s%s:%s%s%s", Cyan, k, Reset, Yellow, v, Reset)
	}

	return ret
}

func printArgs(args map[string]string) string {
	ret := ""
	for k, v := range args {
		ret += fmt.Sprintf("  %s:%s", k, v)
	}

	return ret
}

func colorMultilinePrintArgs(args map[string]string) string {
	if len(args) == 0 {
		return ""
	}

	ret := ""
	for k, v := range args {
		ret += fmt.Sprintf("\n%s%12s%s:%s%s%s", Cyan, k, Reset, Yellow, v, Reset)
	}

	return ret
}
