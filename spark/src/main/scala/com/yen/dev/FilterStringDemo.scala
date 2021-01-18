package com.yen.dev

object FilterStringDemo extends App{

  val words = "this is my words 523 45rt43 2mlrtmpfvwe sfv(*&^&((BIHKJ (&*(*&%&(*^())"
  println(words)

  val words_cleaned = words.replaceAll("[^A-Za-z]+", "")
  println(words_cleaned)
}
