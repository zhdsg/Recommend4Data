package com.eastday.utils

import java.util.regex.Pattern

/**
 * Created by admin on 2018/4/20.
 */
object ETLUtil {
    def isInteger(str:String) :Boolean={
      val pattern =Pattern.compile("^[-\\+]?[\\d]*$")
      pattern.matcher(str).matches()
    }

    def trimDate6( str:String) :String={
      if(str.length() >= 7 && isInteger(str.substring(str.length() - 6, str.length()))) {
          val head:String = str.substring(0, str.length() - 6);
          val tail1:Int = Integer.parseInt(str.substring(str.length() - 6, str.length() - 4));
          val tail2:Int= Integer.parseInt(str.substring(str.length() - 4, str.length() - 2));
          val tail3:Int = Integer.parseInt(str.substring(str.length() - 2, str.length()));
          if( tail1 >= 0 && tail2 >= 1 && tail2 <= 12 && tail3 >= 1 && tail3 <= 31){
            head
          }else{
            str
            }
       } else {
           str
      }
    }
  def trimDate(str:String) :String={
      trimDate6(trimDate6(trimDate6(trimDate6(trimDate6(str)))))
  }

  def trimSpecial(str:String) :String={
    var str1 =str
    if(str.contains("\\")){
      str1 =str.replace("\\","")
    }
    if(str1.contains("'")){
      str1=str1.replace("'","")
    }
    str1
  }

  def filter(str:String):Boolean={
    val pattern =Pattern.compile("[a-z|A-Z|0-9|\\-|_]*$")
    pattern.matcher(str).matches()
  }

  def regexpExtract(x:String , regexp:String,index :Int):String ={
      if(x ==null || x.equals("")){
        return null
      }
      if(regexp == null || regexp.equals("")){
        return regexp
      }

      val pattern = Pattern.compile(regexp);
      val m =pattern.matcher(x)
      if(m.find(index)){
        m.group(index)
      }else{
        null
      }
//      if (isMatch){
//        val regs =regexp.split("\\(")
//
//        if(regs.size <= index){
//          throw new ArrayIndexOutOfBoundsException()
//        }else{
//          val reg = regs(index).substring(0,regs(index).length-1)
//          println(reg.length)
//          pattern=Pattern.compile(reg)
//          pattern.matcher(x).group()
//        }
//      }else {
//        null
//      }
  }
  def main(args: Array[String]) {
      println("ffdcb2ec94b8edc7d214050ab15ab816\u0001180430122447914\u0001180501003741913\u00011525192522")
    //println(regexpExtract("https://mini.eastday.com/mobile/160120110929839.html","(.*?)([a-zA-Z]{0,1}[0-9]{15,})(.html[\\?]{0,1}.*)",2))
  }
//  public static boolean isInteger(String str) {
//        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
//        return pattern.matcher(str).matches();
//    }
//
//  public static String trimDate6(String str) {
//        if(str.length() >= 7 && isInteger(str.substring(str.length() - 6, str.length()))) {
//            String head = str.substring(0, str.length() - 6);
//            int tail1 = Integer.parseInt(str.substring(str.length() - 6, str.length() - 4));
//            int tail2 = Integer.parseInt(str.substring(str.length() - 4, str.length() - 2));
//            int tail3 = Integer.parseInt(str.substring(str.length() - 2, str.length()));
//            return tail1 >= 0 && tail2 >= 1 && tail2 <= 12 && tail3 >= 1 && tail3 <= 31?head:str;
//        } else {
//            return str;
//        }
//    }
//
//  public String evaluate(Object... args) {
//    String str = args[0].toString();
//    return trimDate6(trimDate6(trimDate6(trimDate6(trimDate6(str)))));
//  }

}
