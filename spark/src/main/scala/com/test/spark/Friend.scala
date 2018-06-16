package com.test.spark

trait Friend {
  
   val name: String
   def listen() = println("I " + name + " listening")
   
}