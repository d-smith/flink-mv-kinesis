package org.ds.fmvk.producer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class QuotesProducer {
    static Logger LOG = LoggerFactory.getLogger(QuotesProducer.class);

    public static void main(String... args) throws Exception {

        String[] symbols = {
                "DWAC",
                "SNDL",
                "CEI",
                "FAMI",
                "KMDN"
        };

        StreamWriter streamWriter = new StreamWriter();

        for (; ; ) {

            int idx = (int) (Math.random() * symbols.length);
            String symbol = symbols[idx];
            String price = String.valueOf((Math.random() * 600));
            String quoteString = String.format("%s,%s", symbol, price);

            //LOG.info("quoting {}", quoteString);
            streamWriter.writeToStream("quotestream", UUID.randomUUID().toString(), quoteString.getBytes(StandardCharsets.UTF_8));
            Thread.sleep(150);
        }
    }
}

