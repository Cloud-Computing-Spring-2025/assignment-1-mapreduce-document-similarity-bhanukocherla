package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class JaccardMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(" ", 2); // Splitting document name and text
        if (parts.length < 2) return;

        String docID = parts[0];
        String content = parts[1];

        HashSet<String> words = new HashSet<>();
        StringTokenizer tokenizer = new StringTokenizer(content);

        while (tokenizer.hasMoreTokens()) {
            words.add(tokenizer.nextToken().toLowerCase()); 
        }

        context.write(new Text(docID), new Text(String.join(",", words)));
    }
}
