package com.test.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._;

object StartSpark {
  
  case class Movie(id:String, titel:String, year:String)
   
     org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this);
   
   def main(args: Array[String]) {
     
       
       def writeStuff(writer: Writer) = 
       {
           writer.write("This is stupid");
           println(writer);
       }
       
       writeStuff(new StringWriter)
       writeStuff(new StringWriter with UpperCaseFilter);
       writeStuff(new StringWriter with ProfanityFilter);
       writeStuff(new StringWriter with UpperCaseFilter with ProfanityFilter);
     
       /*var sam = new Human("Sam");
       sam.listen();
       
       val buddy = new Dog("Buddy");
       buddy.listen();
     
       val myCat = new Cat("myCat") with Friend 
       myCat.listen();
       
       var prices = List(10,15, 20, 25, 30, 35, 40);
       
       def totalPrice(prices: List[Int], selector: Int => Boolean) = 
       {
          var total = 0;
          
          for(price <- prices) {
            if(selector(price)) total += price;
          }
          
          total
       }
       
       println("--> " + totalPrice(prices, { prices => prices <20}));
     
       object CarObj {
          def apply(year: Int) = new CarA(year, 0);
       }
       
       val car1 = CarObj.apply(1023);
       
       println(car1.year);
       car1.drive(10);
        println(car1.miles);
       println(factorial(30));
       //myFileTest()
*/    
   }  
   
   def factorial(n: Int) = factorialImp(n: Int, BigInt(1));
   /*{
      
       @scala.annotation.tailrec
       def factorialImp(n: Int, fact: BigInt) : BigInt = 
       {
           if(n == 1)
           fact
           else
             factorialImp(n-1, fact*n)
             
       }
     
   }*/
   
   
   @scala.annotation.tailrec
   def factorialImp(n: Int, fact: BigInt) : BigInt = 
   {
       if(n == 1)
       fact
       else
         factorialImp(n-1, fact*n)
         
   }
   
   def myFileTest()
   {
        var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
        val sc = new SparkContext(conf)
        
        val sqlContext = new SQLContext(sc)
        
        import sqlContext.implicits._
        val textRDD = sc.textFile("file:///C://sparktest//movies.csv");   //xbox_feed.csv"); //movies.csv")
        
        //println(textRDD.foreach(println)
        val empRdd = textRDD.map {
          line =>
            val col = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    Movie(col(0), col(1), col(2));
        }
        
        val empDF = empRdd.toDF();
        empDF.show(20, false)
        
        empDF.select("id", "titel").show(10);
        empDF.filter($"year" > 2009).show(10, false);
        
        val cnt = empDF.groupBy("id").count();
        
        println("-- cnt--" + cnt);
        
       // empDF.filter($"titel" === "Hanna").show(10, false);
        
       // val empDFSHort = empDF.sort($"id");
       // empDFSHort.show();
        
        //empDF.filter("titel == '10 Sekunden'").show();
        
       // empDF.filter(not(empDF("titel").rlike("10"))).show(50, false);
    
        //val searchRDD = empRdd.filter(x=> x.titel == "Flashback");
        
       // println(searchRDD.collect());
        
       // println(searchRDD.count());
        //searchRDD.toDF().show(false);
        
       // println(empRdd.count());
     
   }
}