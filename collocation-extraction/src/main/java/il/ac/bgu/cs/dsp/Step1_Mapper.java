package il.ac.bgu.cs.dsp;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step1_Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // FULL MERGED STOP WORDS LIST
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", 
        "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", 
        "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", 
        "are", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", 
        "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", 
        "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "computer", 
        "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", 
        "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", 
        "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", 
        "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", 
        "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", 
        "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", 
        "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", 
        "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", 
        "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", 
        "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", 
        "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", 
        "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", 
        "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", 
        "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", 
        "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", 
        "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", 
        "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", 
        "these", "they", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", 
        "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", 
        "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", 
        "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", 
        "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", 
        "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves",
        "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ", 
        "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר", 
        "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", 
        "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם", 
        "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", 
        "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", 
        "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", 
        "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים", 
        "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", 
        "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
    ));

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        
        if (parts.length >= 3) {
            String word = parts[0].trim();
            
            // FILTER: Remove Garbage
            if (word.length() < 2 || !word.matches("^[a-zA-Z\u0590-\u05FF]+$")) {
                return;
            }

            // FILTER: Remove Stop Words
            if (STOP_WORDS.contains(word.toLowerCase())) {
                return;
            }

            try {
                long count = Long.parseLong(parts[2]);
                context.write(new Text(word), new LongWritable(count));
            } catch (NumberFormatException e) {}
        }
    }
}