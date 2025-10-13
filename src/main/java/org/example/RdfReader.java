package org.example;

import org.apache.commons.io.FilenameUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.function.Consumer;

public class RdfReader {

    public static void streamRdf(String filename, Consumer<Triplet> handler) throws Exception {
        String ext = FilenameUtils.getExtension(filename).toLowerCase();
        Lang lang;
        switch (ext) {
            case "rdf": lang = Lang.RDFXML; break;
            case "ttl": lang = Lang.TURTLE; break;
            case "nt":  lang = Lang.NTRIPLES; break;
            default: throw new IllegalArgumentException("Unsupported RDF format: " + filename);
        }

        try (InputStream in = new FileInputStream(filename)) {
            StreamRDF sink = new StreamRDFBase() {
                @Override
                public void triple(Triple t) {
                    try {
                        Node subjNode = t.getSubject();
                        Node predNode = t.getPredicate();
                        Node objNode  = t.getObject();

                        String subj = subjNode.toString();
                        String pred = predNode.toString();
                        String obj  = objNode.toString();

                        handler.accept(new Triplet(subj, pred, obj));
                    } catch (Exception e) {
                        //
                    }
                }
            };

            RDFParser.create()
                    .source(in)
                    .lang(lang)
                    .parse(sink);
        }
    }
}
