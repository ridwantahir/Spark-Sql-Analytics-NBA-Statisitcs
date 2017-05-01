package bigdata.NBAStats

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import java.util.regex.Pattern
import org.jsoup.Jsoup
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import java.util.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

class NameExtractor {
  
  def apply(spark:SparkSession):Unit={
    import spark.implicits._
    val path="dump";
    val data=spark.read.text(path).as[String];
    /*
     * The objective of the following filtering is to read the html documents and extracts links that 
     * will direct to the correct wikipedia article. the code looks for lines that look  like the following html tag:
     * <li><i><a href="https://en.wikipedia.org/wiki/Stan_Zadel" title="Stan Zadel">Stan Zadel</a></i></li>
     */
    
    
    // use the Java regex matching enigne
    var pattern=Pattern.compile("<li>.*<a\\s+href=\"(http[^\\s^\\?]+)\"\\s*.+>([a-zA-Z0-9\\s]{2,})<.*/li>$");   
    val filtered=data.filter(x=>{
      val matcher=pattern.matcher(x);
      matcher.find();
    })
    // from each of the filtered lines in the html document, extract the link address and the player name
    val mapped=filtered.map(x=>{
      val matcher=pattern.matcher(x);
      if(matcher.find()){
        (matcher.group(1), matcher.group(2))
      }
      else ("noth","noth");
    });
    /*
     * IN the following couple of code lines, we read each of the links from the 
     * mapped dataset( which is dataset of (Player name, link to player's wikipedia article), read the link and 
     * load the html content of the respective page. then select the section with class id of infobox vcard. 
     * then go through each html line and match the data in the html and store it
     */
      val html=mapped.map(line=>{
        // the following are the information we search for in the article
      var points:String=null;
      var rebounds:String=null;
      var assists:String=null;
      var height:String=null;
      var weight:String=null;
      var nationality:String=null;
      var born:String=null;
      var playingcareer:String=null;
      var position:String=null;
      // read the html article from wikipedia
      var html = scala.io.Source.fromURL(line._1).mkString
      // use regex to extract the require section. the required section is a table with class name infobox vcard
      var patternStat=Pattern.compile("<table class=\"infobox vcard\".*?>([\\s\\S]+?)</table>");
      var matcherStat=patternStat.matcher(html);
      if(matcherStat.find()){// if the section is found in the page
        // result  contains the table we are querying
        val result=matcherStat.group(1);
        val restoList=result.split("\\n");
        val info=List();
        // go through each entry in the table row
        for(i<- 0 until restoList.size){
          /*
           * search for raws that starts with <th> and match it to the information/entry we need
           * use regex to search for the following format
           * <th scope="row">INforamtion</th>
           */
          var heading=restoList(i);
          /*
           * use regex to read the value corresponding to the information/entry above. 
           */
          val thPattern=Pattern.compile("<th.*?>(.+?)</th>");
          val thmatcher=thPattern.matcher(heading);
          if(thmatcher.find()){
            // use JSOUP to read the html text of the body of the elements in <tr>
            val title= Jsoup.parse(heading).body().text;
            var value:String=null;
            if(i+1<restoList.size) value=restoList(i+1);
            val dataextract= Jsoup.parse(value).body().text();
            // and the store the inforamtion we need
            if(title.toLowerCase().contains("Point".toLowerCase())){points=dataextract}
            if(   title.toLowerCase().contains("Rebound".toLowerCase())){rebounds=dataextract}
            if(   title.toLowerCase().contains("Assist".toLowerCase())){assists=dataextract}
            if(   title.toLowerCase().contains("height".toLowerCase())){height=dataextract}
            if(   title.toLowerCase().contains("weight".toLowerCase())){weight=dataextract}
            if(   title.toLowerCase().contains("Nationality".toLowerCase())){nationality=dataextract}
            if(   title.toLowerCase().contains("Born".toLowerCase())){born=dataextract}
            if(   title.toLowerCase().contains("Playing career".toLowerCase())){playingcareer=dataextract}
            if(   title.toLowerCase().contains("Position".toLowerCase())){position=dataextract}
           
          }
        }
        // emit the tuple
        (line._2,nationality,born,height,weight,playingcareer,position,points,rebounds,assists,line._1)
      }
      else {
        // emit default tuple if the page can't be parsed
        (line._2,nationality,born,height,weight,playingcareer,position,points,rebounds,assists,line._1);
      }
    });
      // column processing function to extract birth date from unstructured information in the previous step
    val extractDate=(bdate:String)=>{
      if(bdate==null){
        null;
      }
      else{
        // there are two different formats in which the date of birth info is displayed on the wikipidia pages
        val pattern=Pattern.compile("(\\d{4}-\\d{2}-\\d{2})" );// formats like 1994-06-21
        val pattern2=Pattern.compile("([a-zA-Z]+\\s+\\d{1,2}\\s*,\\s*\\d{4})" );// formats like : January 24, 2010
        val matcher=pattern.matcher(bdate);  
        val matcher2=pattern2.matcher(bdate);
        if(matcher.find()){
          matcher.group(1);
    }
    else if(matcher2.find()){
       matcher2.group(1);
    }
    else{
      null
    }
      }
  }
     // column processing function to extract weight from unstructured information in the previous step
    val weightModifier =(weighti:String)=>{
      if(weighti==null){
        null;
      }
      else{
        val spaceiInt=160
        val space=spaceiInt.toChar
        var weight=weighti.replaceAll(space+"","");
        val pattern1=Pattern.compile("\\d{1,3}\\s*lb\\s*\\((\\d{1,3})\\s*kg\\)");// formats like 270 lb (122 kg)
        val pattern2=Pattern.compile("(\\d{1,3})\\s*kg")// formats like 120 kg (200 lb)
        val matcher1=pattern1.matcher(weight);
        val matcher2=pattern2.matcher(weight);
        if(matcher1.find()){          
          Option(matcher1.group(1).toDouble);
        }
        else if(matcher2.find()){         
          Option(matcher2.group(1).toDouble);
        }
        else{
          null;
        }
      }
    }
    // column processing function to extract total statistics from unstructured information in the previous step
    val totalStatisticsExtractor=(value:String)=>{
      if(value==null){null}
      else{
        val comaRemoved=value.replaceAll(",", "");
        val pattern=Pattern.compile("(\\d+)\\s*\\(\\d+\\.\\d+\\s*[pra]pg\\)");// format like 964 (2.1 apg)
        val matcher1=pattern.matcher(comaRemoved);
        if(matcher1.find()){
          Option(matcher1.group(1).toDouble);
        }
        else
        {
          null
        }
      }
    }
    // column processing function to extract per game statistics from unstructured information in the previous step
    val perGameStatisticsExtractor=(value:String)=>{
       if(value==null){null}
      else{
        val comaRemoved=value.replaceAll(",", "");
        val pattern=Pattern.compile("\\d+\\s*\\((\\d+\\.\\d+)\\s*[pra]pg\\)");// format like 964 (2.1 apg)
        val matcher1=pattern.matcher(comaRemoved);
        if(matcher1.find()){
         Option( matcher1.group(1).toDouble);
        }
        else
        {
          null
        }
      }
    }
    val nationalitySplitter=(nationality:String)=>{
      if(nationality==null)null
      else
      nationality.split("/").map { x => x.trim() }.mkString(",")
    }
     // column processing function to extract height in metre from unstructured information in the previous step
    val heightModifier=(heighti: String)=>{//[196cm (6ft 5in)]     [1.96m (6ft 5in)] 
      if(heighti==null){
         null;
      }else{
        val spaceiInt=160
        val space=spaceiInt.toChar
        var height=heighti.replaceAll(space+"","");
        // format like 6 ft 11 in (2.11 m)
        val pattern= Pattern.compile("\\d\\s*ft\\s*\\d{1,2}\\s*in\\s*\\(\\s*(\\d+\\.\\d{1,2}\\s*)m\\s*\\)");
        val pattern2=Pattern.compile("(\\d{1,3})cm\\s*\\(");
        val pattern3=Pattern.compile("(\\d\\.\\d{1,2})m");
        val matcher=pattern.matcher(height);
        val matcher2=pattern2.matcher(height);
        val matcher3=pattern3.matcher(height);
        if(matcher.find()){
          Option(matcher.group(1).toDouble);
          
        }
        else if(matcher2.find()){
          Option(matcher2.group(1).toDouble/100);
        }
        else if(matcher3.find()){
          Option(matcher3.group(1).toDouble);
        }
        else
        {
          null;
        }       
                 
      }
    }
    // convert dataset to dataframe
      val htmldaaset=html.toDF("Name","nationality", "born","height","weight","playingcareer","position","points","rebounds","assists", "Link")
      // define udfs from the above function
      val dateudf=udf(extractDate);
      val heightudf=udf(heightModifier);
      val weightudf=udf(weightModifier);
      val nationalityudf=udf(nationalitySplitter);
      val totalStatudf=udf(totalStatisticsExtractor);
      val perGameudf=udf(perGameStatisticsExtractor);
      //now create and append the new columns( with information in the correct format) to the dataset and drop the old columns
      val trasnformedData=htmldaaset.withColumn("Birth Date", dateudf(col("born")))
                                    .withColumn("Height (meter)", heightudf(col("height")))
                                    .withColumn("Weight (Kg)", weightudf(col("weight")))
                                    .withColumn("Nationalities", nationalityudf(col("nationality")))
                                    .withColumn("Total points", totalStatudf(col("points")))
                                    .withColumn("Per Game Point", perGameudf(col("points")))
                                    .withColumn("Total Rebounds", totalStatudf(col("rebounds")))
                                    .withColumn("Per Game Rebounds", perGameudf(col("rebounds")))
                                    .withColumn("Total Assists", totalStatudf(col("assists")))
                                    .withColumn("Per Game Assists", perGameudf(col("assists")))
                                    .drop("nationality", "born","height","weight","playingcareer","position","points","rebounds","assists");
      //save as csv
      trasnformedData.repartition(1).write.mode(SaveMode.Overwrite).options(Map("delimiter"->",", "header"->"true")).csv("result"); 
  }
  
  
  
  
  
  
  
  
}