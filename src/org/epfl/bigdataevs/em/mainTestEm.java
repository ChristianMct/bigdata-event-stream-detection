package org.epfl.bigdataevs.em;

import org.apache.commons.math3.fraction.BigFraction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.epfl.bigdataevs.eminput.ParsedArticle;
import org.epfl.bigdataevs.eminput.TimePeriod;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class mainTestEm{
  public static void main(String[] args) {
    Date start = new Date();
    Date end = new Date();
    
    String article1 = "Dans ce texte remis à François Hollande ce mercredi, le président de l'Assemblée nationale juge par ailleurs que «le sentiment d'appartenance républicaine» n'est lié ni au mode d'acquisition de la nationalité, ni à la religion. Contrairement au président du Sénat, Gérard Larcher (UMP), qui a remis mercredi un rapport sur le même thème au chef de l'Etat, Claude Bartolone n'aborde pas la question de l'organisation des cultes. Lors des auditions et déplacements qu'il a effectués, relève-t-il, «le sujet des appartenances religieuses, des origines familiales ou de l'actualité de la loi de 1905 (sur la laïcité) n'est jamais apparu spontanément». «Réaffirmer le sentiment d'appartenance républicaine par la citoyenneté, c'est aussi redire que ce n'est pas une question de modalités d'acquisition de la nationalité, que ce n'est pas non plus une question d'origine ou cultuelle», écrit-il. Si certains sont à l'écart de ce sentiment d'appartenance, comme «les absents des marches des 10 et 11 janvier», ce n'est pas lié à «la religion des uns ou des autres» mais au fait «que notre République est aujourd'hui malade de phénomènes de repli, de cloisonnement, d'entre-soi». Dans son texte, Claude Bartolone rappelle que les députés socialistes avaient déposé en 2003 une proposition de loi pour le vote obligatoire. «Je soutiendrai comme il y a 12 ans cette proposition: la citoyenneté, c'est un droit mais c'est aussi un devoir».";
    String article2 = "C'était attendu, c'est officiel: Bruxelles accuse Google d'abus de position dominante dans les moteurs de recherche. Par ailleurs, la Commission européenne a aussi ouvert une enquête contre Google sur son système pour téléphone portable Android.. «Je crains que l'entreprise n'ait injustement avantagé son propre service de comparaison de prix, en violation des règles de l'UE en matière d'ententes et d'abus de position dominante», a déclaré la commissaire européenne chargée du dossier, Margrete Vestager. Concrètement, la Commission craint que les utilisateurs de Google, qui représente 90% des recherches sur l'internet dans la plupart des pays d'Europe, «ne voient pas nécessairement les résultats les plus pertinents en réponse à leurs requêtes».";
    String article3 = "C'est un évènement. Le président français François Hollande est attendu ce mercredi en Suisse pour une visite d'Etat de deux jours qui vise à relancer la relation avec Berne. Et ce, 17 ans après Jacques Chirac, dernier président français en date à s'être rendu en visite officielle chez le voisin helvétique. Paris entend ainsi tourner la page d'une série de brouilles sur les questions de l'évasion fiscale ou de l'aéroport de Bâle-Mulhouse mais aussi célébrer voire s'inspirer du «modèle» helvétique en matière de croissance verte ou d'apprentissage.  Après avoir accepté d'échanger, sur demande, les informations concernant les ressortissants français soupçonnés d'évasion fiscale par Bercy, Berne s'est engagé à rendre cet échange d'informations automatique à compter de 2018. A son arrivée sur le sol helvétique où il est attendu vers 14h, François Hollande, accompagné de six membres du gouvernement (Ecologie, Education, Finances, Travail, Affaires européennes et Numérique) sera accueilli par la présidente en exercice de la Confédération, Simonetta Sommarruga. Tous deux s'exprimeront ensuite à l'hôtel de ville de Berne avant de se retrouver pour un entretien et de tenir une conférence de presse conjointe. Un « dîner d\'Etat » les réunira une nouvelle fois dans la soirée.";
  
    
    article1 = "bigdata science";
    article2 = "computer science";
    article3 = "communication systems";
    
    
    String[] words1 = article1.split(" ");
    String[] words2 = article2.split(" ");
    String[] words3 = article3.split(" ");
    
    HashMap<String, Double> backgroundModel = new HashMap<>();
    ArrayList<String> allWords = new ArrayList<>();
    for (String w : words1) {
      allWords.add(w);
    }
    for (String w : words2) {
      allWords.add(w);
    }
    for (String w : words3) {
      allWords.add(w);
    }
    
    for(String w: allWords) {
      if(backgroundModel.containsKey(w)) {
        Double value = backgroundModel.get(w) * allWords.size();
        backgroundModel.put(w, value + allWords.size());
      } else {
        backgroundModel.put(w, 1.0 / allWords.size());
      }
    }
    
    ParsedArticle parsedArticle1 = new ParsedArticle(countWords(words1), null);
    ParsedArticle parsedArticle2 = new ParsedArticle(countWords(words2), null);
    ParsedArticle parsedArticle3 = new ParsedArticle(countWords(words3), null);
    
    ArrayList<ParsedArticle> articles = new ArrayList<>();
    ArrayList<EmInput> partitions = new ArrayList<>();
    articles.add(parsedArticle1);
    articles.add(parsedArticle2);
    articles.add(parsedArticle3);
    EmInput input = new EmInput(backgroundModel, articles, new TimePeriod(start, end));
    partitions.add(input);
    
    JavaSparkContext sc = new JavaSparkContext("local", "EM Algorithm Test");
    EmAlgo emAlgo = new EmAlgo(sc.parallelize(partitions), 3, 0.8);
    Map<Theme, Double> result =  (Map<Theme, Double>) emAlgo.algo().collectAsMap();
    sc.close();
    System.out.println(result.keySet().size() + " elements");
    int i = 0;
    for (Theme theme : result.keySet()) {
      System.out.println("Theme :"+i);
      System.out.println(theme.sortString());
      System.out.println("Score:" + result.get(theme));
      i += 1;
    }
    System.out.println("Done !");
  }

  
  public static HashMap<String, Integer> countWords(String[] words) {
    HashMap<String, Integer> map = new HashMap<>();
    for (String w : words) {
      if(map.containsKey(w)) {
        int val = map.get(w);
        map.put(w, val+1);
      } else {
        map.put(w, 1);
      }
    }
    return map;
  }
}
