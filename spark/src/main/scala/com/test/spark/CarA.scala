package com.test.spark

class CarA(val year:Int, var miles: Int)
{
    println("created .." ); 
    
    def drive(dist: Int) = {
      
        println("driving");
        
        miles +=  dist;
    }
}

