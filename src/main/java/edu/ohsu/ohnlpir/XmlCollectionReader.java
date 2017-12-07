package edu.ohsu.ohnlpir;


import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.core.resource.FileLocator;
import org.apache.ctakes.typesystem.type.structured.DocumentID;
import org.apache.uima.UIMAFramework;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.component.initialize.ConfigurationParameterInitializer;
import org.apache.uima.fit.component.initialize.ExternalResourceInitializer;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.IOException;
import java.util.List;

// This thing's job is to read an OHSUCLIN XML file and get it into cTAKES-land.
// The files look like:
// <main>
//   <DATA_RECORD>
//      ...
//      <NOTE_TEXT>...</NOTE_TEXT>
//   </DATA_RECORD>
//   ...
//   <main>
//
// Relevant fields:
// NOTE_TEXT, which has the actual we want to cTAKES-ify
// SOURCE_SYSTEM_NOTE_CSN_ID, which will be our doc id


@PipeBitInfo(
        name = "OHSU XML File Reader",
        description = "Reads files in the OHSU xml format, produces JCAS.",
        role = PipeBitInfo.Role.READER,
        products = {PipeBitInfo.TypeProduct.DOCUMENT_ID }
)

public class XmlCollectionReader extends JCasCollectionReader_ImplBase {

    @ConfigurationParameter(
            name = "InputFile",
            description = "Path to input XML file."
    )
    private String inputFilePath;

    protected int currentIdx;

    private Document thisXmlDoc;
    private List<Element> records;

    private Logger logger;

    @Override
    public void initialize(final UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        ConfigurationParameterInitializer.initialize(this, getUimaContext());
        ExternalResourceInitializer.initialize(this, getUimaContext());


        logger = UIMAFramework.getLogger(XmlCollectionReader.class);

        String zebraVal = (String)getConfigParameterValue("Zebra");

        logger.log(Level.INFO, "Zebra config param is: " + zebraVal);

        logger.log(Level.INFO, "In Init for XML collection reader!");

        logger.log(Level.INFO, "In Init for XML Collection Reader, inputFilePath is: " + inputFilePath);

        File infile;
        try {
            infile = FileLocator.locateFile(inputFilePath);
        } catch (IOException ex) {
            // TODO: Maybe make our own exceptoin instead of using DirectoryNotFound?
            throw new ResourceInitializationException(ex);
        }

        if (!infile.exists() || !infile.canRead()) {
            // TODO: set up custom error here
            throw new ResourceInitializationException(ResourceConfigurationException.DIRECTORY_NOT_FOUND,
                    new Object[] {inputFilePath, getMetaData().getName(), inputFilePath});
        }

        try {

            SAXBuilder builder = new SAXBuilder();

            thisXmlDoc = builder.build(infile);

            records = thisXmlDoc.getRootElement().getChildren("DATA_RECORD");

        } catch  (IOException ex) {
            // TODO: custom exception here
            throw new ResourceInitializationException(ResourceConfigurationException.DIRECTORY_NOT_FOUND,
                    new Object[] {inputFilePath, getMetaData().getName(), inputFilePath});
        } catch  (JDOMException ex){
            // TODO: custom xml parsing exception here
            throw new ResourceInitializationException(ResourceConfigurationException.DIRECTORY_NOT_FOUND,
                    new Object[] {inputFilePath, getMetaData().getName(), inputFilePath});
        }


    }


    public void getNext(final JCas jcas) throws IOException, CollectionException {

        // get the relevant doc out of records
        Element this_doc = records.get(this.currentIdx);

        // pull out the note and doc id fields
        Element noteTextEl = this_doc.getChild("NOTE_TEXT");
        String noteText = noteTextEl.getText();

        Element docIdEl = this_doc.getChild("SOURCE_SYSTEM_NOTE_CSN_ID");
        String docId = docIdEl.getText();

        DocumentID docIdAnnotation = new DocumentID(jcas);
        docIdAnnotation.setDocumentID(docId);
        docIdAnnotation.addToIndexes();

        jcas.setDocumentText(noteText);


        this.currentIdx += 1;
    }


    public void close() throws IOException {
    }


    public boolean hasNext() {
        return this.currentIdx < this.records.size();
    }

    public Progress[] getProgress() {

        return new Progress[]{
            new ProgressImpl(this.currentIdx, this.records.size(), Progress.ENTITIES)

        };


    }

}
