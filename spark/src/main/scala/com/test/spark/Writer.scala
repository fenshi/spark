package com.test.spark

abstract class Writer {
  
  def write(msg: String) 
}

class StringWriter extends Writer 
{
    val target = new StringBuilder;
    
    def write(msg: String) = {
      target.append(msg);
    }
     
    override def toString = target.toString();
    
}

trait UpperCaseFilter extends Writer 
{
    abstract override def write(msg: String) = {
      
        //next one in chain
        super.write(msg.toUpperCase())
    }
}

trait ProfanityFilter extends Writer 
{
    abstract override def write(msg: String) = {
      
        //next one in chain
        super.write(msg.replace("stupid", "s****"));
    }
}


