package edu.ohsu.ohnlpir;

import com.lexicalscope.jewel.cli.CliFactory;
import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.CliOptionals;
import org.apache.ctakes.core.pipeline.PipelineBuilder;
import org.apache.ctakes.core.pipeline.PiperFileReader;
import org.apache.log4j.Logger;
import org.apache.uima.UIMAException;

import java.io.IOException;

public class PipelineRunner {

    static private final Logger LOGGER = Logger.getLogger("OHSU PipelineRunner");

    /*
    The point of this class is to be a version of PiperFileRunner that actually passes parameters to the reader.
     */
    public static void main(String[] args) {

        final CliOptionals options = CliFactory.parseArguments(CliOptionals.class, args);

        try {
            final PiperFileReader reader = new PiperFileReader();
            final PipelineBuilder builder = reader.getBuilder();

            // input file?
            final String infile = options.getOption_x();
            if (!infile.isEmpty()) {
                builder.set(XmlCollectionReader.PARAM_INPUTFILE, infile);
            }

            final String outputDir=options.getOutputDirectory();
            if (!outputDir.isEmpty()) {
                builder.set(ConfigParameterConstants.PARAM_OUTPUTDIR, outputDir);
            }

            // figure out UMLS stuff from args
            final String umlsUser = options.getUmlsUserName();
            final String umlsPass = options.getUmlsPassword();

            if (!umlsUser.isEmpty()) {
                builder.set("umlsUser", umlsUser);
                builder.set("ctakes.umlsuser", umlsUser);
            }

            if (!umlsPass.isEmpty()) {
                builder.set("umlsPass", umlsPass);
                builder.set("ctakes.umlspw", umlsPass);
            }

            // now load the piper file
            reader.setCliOptionals(options);
            reader.loadPipelineFile(options.getPiperPath());

            // now, loadPipelineFile() WILL NOT pass any configuration args to the reader,
            // so we manually must set it...
            builder.reader(XmlCollectionReader.class, XmlCollectionReader.PARAM_INPUTFILE, infile);

            builder.run();

        } catch (UIMAException e) {
            LOGGER.error("UIMA Exception: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            LOGGER.error("IOException loading piper file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }
}
