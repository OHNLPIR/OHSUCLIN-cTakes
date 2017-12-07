package edu.ohsu.ohnlpir;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.ctakes.core.cc.XMISerializer;
import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.core.util.DocumentIDAnnotationUtil;
import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.component.CasConsumer_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import org.apache.commons.compress.archivers.tar.*;
import org.xml.sax.SAXException;

import java.io.*;


@PipeBitInfo(
        name = "tar.gz collection writer",
        description = "Writes CAS XMI files to a .tar.gz file.",
        role = PipeBitInfo.Role.WRITER,
        dependencies = { PipeBitInfo.TypeProduct.DOCUMENT_ID }
)

public class TarFileXMICollectionWriter extends CasConsumer_ImplBase {

    static private Logger LOGGER = UIMAFramework.getLogger(TarFileXMICollectionWriter.class);

    private final static String PARAM_OUTFILE_NAME = "OutfileName";
    private final static String DESC_OUTFILE = "The path to the output TAR file.";

    @ConfigurationParameter(
            name = PARAM_OUTFILE_NAME,
            description = DESC_OUTFILE,
            mandatory = true
    )
    private File outfile;

    @ConfigurationParameter(
            name = ConfigParameterConstants.PARAM_OUTPUTDIR,
            description = ConfigParameterConstants.DESC_OUTPUTDIR,
            mandatory = true
    )
    private File outputRootDir;




    private TarArchiveOutputStream tarOut;
    private CompressorOutputStream zipOut;

    private int docCount;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        LOGGER.log(Level.INFO, "In  TarFileXMICollectionWriter.initialize(), about to call super.initialize()");
        super.initialize(context);
        LOGGER.log(Level.INFO, "In  TarFileXMICollectionWriter.initialize()");



//        final String outputDir = (String)context.getConfigParameterValue(ConfigParameterConstants.PARAM_OUTPUTDIR);
        final String outputFilePath = (String)context.getConfigParameterValue(PARAM_OUTFILE_NAME);


        LOGGER.log(Level.INFO, "outdir: " + outputRootDir);
        LOGGER.log(Level.INFO, "outfile: " + outputFilePath);

        LOGGER.log(Level.INFO, "zebras: " + context.getConfigParameterValue("Zebra"));

        outfile = new File(outputRootDir, outputFilePath);

        try {
            FileOutputStream fos = new FileOutputStream(outfile);
            zipOut = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP, fos);
            tarOut = new TarArchiveOutputStream(zipOut);

        } catch (FileNotFoundException e) {
            // TODO: do something here
            e.printStackTrace();
            throw new ResourceInitializationException("Trouble making FileInputStream", new Object[] {PARAM_OUTFILE_NAME});
        } catch (CompressorException e) {
            // TODO: do something here
            e.printStackTrace();

            throw new ResourceInitializationException("CompressorException?", new Object[] {PARAM_OUTFILE_NAME});
        }


    }


    @Override
    public void process(final CAS cas) throws AnalysisEngineProcessException {
        LOGGER.log(Level.INFO, "Here, in TarFileXMICollectionWriter.process()!");

        // 1. Serialize the CAS to XML in a byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        XmiCasSerializer casSerializer = new XmiCasSerializer( cas.getTypeSystem() );
        XMISerializer xmiSerializer = new XMISerializer( baos );
        try {
            casSerializer.serialize( cas, xmiSerializer.getContentHandler() );
        } catch (SAXException e) {
            e.printStackTrace();
            throw new AnalysisEngineProcessException();

        }

        // 2. Compute a "file name"
        String documentId;
        try {
            documentId = DocumentIDAnnotationUtil.getDocumentIdForFile( cas.getJCas() );
        } catch (CASException e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Can't figure out doc id?");
            throw new AnalysisEngineProcessException();
        }

        // 3. Add those bytes to the tar output

        String fname = String.format("%s.xmi", documentId);
        LOGGER.log(Level.INFO, "About to try and process document " + fname + " (count: " + docCount + ")");

        TarArchiveEntry entry = new TarArchiveEntry(fname);

        entry.setSize(baos.size());
        try {
            tarOut.putArchiveEntry(entry);
            tarOut.write(baos.toByteArray());
            tarOut.closeArchiveEntry();
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Couldn't add to tar file...");
            throw new AnalysisEngineProcessException();
        }

        // 4. update book-keeping count of files processed

        docCount += 1;

    }

    public void collectionProcessComplete() {
        LOGGER.log(Level.INFO, "In  TarFileXMICollectionWriter.collectionProcessComplete()");

        try {
            tarOut.close();
            zipOut.close();

        } catch (IOException e) {
            // TODO: stop doing stuff
            e.printStackTrace();
        }

    }
}