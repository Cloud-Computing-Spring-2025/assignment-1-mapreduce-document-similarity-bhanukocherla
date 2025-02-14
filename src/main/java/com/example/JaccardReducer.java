package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class JaccardReducer extends Reducer<Text, Text, Text, Text> {
    private final List<Map.Entry<String, Set<String>>> documents = new ArrayList<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            Set<String> wordSet = new HashSet<>(Arrays.asList(value.toString().split(",")));
            documents.add(new AbstractMap.SimpleEntry<>(key.toString(), wordSet));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < documents.size(); i++) {
            for (int j = i + 1; j < documents.size(); j++) {
                String docA = documents.get(i).getKey();
                String docB = documents.get(j).getKey();
                Set<String> wordsA = documents.get(i).getValue();
                Set<String> wordsB = documents.get(j).getValue();

                Set<String> intersection = new HashSet<>(wordsA);
                intersection.retainAll(wordsB);

                Set<String> union = new HashSet<>(wordsA);
                union.addAll(wordsB);

                double jaccardSimilarity = (double) intersection.size() / union.size();

                context.write(new Text(docA + ", " + docB), new Text("Similarity: " + String.format("%.2f", jaccardSimilarity)));
            }
        }
    }
}
