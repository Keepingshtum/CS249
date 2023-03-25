package edu.sjsu.cs249.zoo;

import picocli.CommandLine;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;

public class CliUtil extends CommandLine {
    static private boolean debugEnabled;
    static public void enableDebug(boolean enable) {
        debugEnabled = enable;
    }

    static private boolean timestampEnabled;
    static public void enableTimestamp(boolean enabled) {
        timestampEnabled = enabled;
    }

    static private CliUtil cli;

    public CliUtil(Object command) {
        super(command);
        cli = this;
    }

    static private int getScreenWidth() {
        return cli.getCommandSpec().usageMessage().width();
    }

    static private void coloredOut(String color, String format, Object... args) {
        var rawMessage = MessageFormat.format(format, args);
        var stylizedMessage = CommandLine.Help.Ansi.AUTO.string(MessageFormat.format("{0} @|{1} {2}|@",
                timestampEnabled ? new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date()) : "",
                color, rawMessage));
        var line = new Help.Column(getScreenWidth(), 0, Help.Column.Overflow.WRAP);
        var txtTable = Help.TextTable.forColumns(Help.defaultColorScheme(Help.Ansi.AUTO), line);
        txtTable.indentWrappedLines = 0;
        txtTable.addRowValues(stylizedMessage);
        System.out.print(txtTable);
        System.out.flush();
    }

    static public void fatal(String format, Object... args) {
        error(format, args);
        System.exit(2);
    }

    static public void error(String format, Object... args) {
        coloredOut("red", format, args);
    }

    static public void simpleError(String err) {
        error("{0}", err);
    }

    static public void warn(String format, Object... args) {
        coloredOut("yellow", format, args);
    }

    static public void info(String format, Object... args) {
        coloredOut("blue", format, args);
    }

    static public void debug(String format, Object... args) {
        if (!debugEnabled) return;
        coloredOut("magenta", format, args);
    }
}
