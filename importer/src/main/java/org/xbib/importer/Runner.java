package org.xbib.importer;

import org.xbib.content.settings.Settings;
import org.xbib.content.settings.SettingsLoader;
import org.xbib.content.settings.SettingsLoaderService;
import org.xbib.importer.util.ExceptionFormatter;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.xbib.content.settings.Settings.settingsBuilder;

/**
 *
 */
public class Runner {

    /**
     * The Runner is a tricky class. It must contain only one method {@link #main(String[])}
     * to get started flawlessly by the java command.
     * It must not contain statics, other constructors, or subclasses.
     * <p>
     * After that, we load our real class from here and start it.
     * We can specifiy the class name on the command line or in a JSON
     * specifcation file. This is only possible as long as the Settings
     * machinery is not using log4j initialization or logging - otherwise, our
     * ConfigurationFactory construction will break here at import time.
     * <p>
     * As a special, multiple JSON specifications can be executed one after another in a
     * chain, such as
     * <p>
     * java {jsondef} {jsondef} {jsondef} ...
     * <p>
     * or pairwise like
     * <p>
     * java {{class} {jsondef}} {{class} {jsondef}} ...
     *
     * @param args the command line args
     * @throws Exception if program fails
     */
    public static void main(String[] args) throws Exception {
        int exitcode = 0;
        try {
            if (args == null || args.length == 0) {
                throw new IllegalArgumentException("no arguments passed, unable to process");
            }
            Program program = null;
            int i = 0;
            while (i < args.length) {
                try {
                    Class<?> clazz = Class.forName(args[i]);
                    program = (Program) clazz.newInstance();
                    i++;
                } catch (Exception e) {
                    // try again to load class, but skip main class from a possible java -cp execution
                    if (i + 1 < args.length && !args[i].endsWith(".json")) {
                        i++;
                    }
                    try {
                        Class<?> clazz = Class.forName(args[i]);
                        program = (Program) clazz.newInstance();
                        i++;
                    } catch (Exception e2) {
                    }
                }
                // now the json specification
                String arg = ".json";
                InputStream in = System.in;
                if (i < args.length) {
                    arg = args[i++];
                    try {
                        URL url = new URL(arg);
                        in = url.openStream();
                    } catch (MalformedURLException e) {
                        in = new FileInputStream(arg);
                    }
                }
                try (Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                    // load settings from JSON
                    SettingsLoader settingsLoader = SettingsLoaderService.loaderFromResource(arg);
                    Settings settings = settingsBuilder()
                            .put(settingsLoader.load(Settings.copyToString(reader)))
                            .replacePropertyPlaceholders()
                            .build();
                    // advanced setting: set up program from JSON if not already set
                    if (settings.containsSetting("program.class")) {
                        Class<?> clazz = Class.forName(settings.get("program.class",
                                "org.xbib.importer.Importer"));
                        program = (Program) clazz.newInstance();
                    }
                    // run program
                    if (program != null) {
                        exitcode = program.run(settings);
                    }
                }
                if (exitcode != 0) {
                    break;
                }
            }
        } catch (Throwable e) {
            System.err.println(ExceptionFormatter.format(e));
            System.exit(1);
        }
        System.exit(exitcode);
    }
}
