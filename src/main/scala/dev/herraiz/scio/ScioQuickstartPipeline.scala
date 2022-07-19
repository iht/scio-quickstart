/*
 * Copyright 2022 Israel Herraiz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.herraiz.scio

import com.spotify.scio._
import com.spotify.scio.values.SCollection

object ScioQuickstartPipeline {
  val thingsToRemove: List[Char] = List(',', '.', '?', '!', '-', '_', '¿', '¡')
  val swapMap: Map[Char, Char] = Map(
    'á' -> 'a',
    'é' -> 'e',
    'í' -> 'i',
    'ó' -> 'o',
    'ú' -> 'u')


  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    val inputFile: String = args("input-file")
    val numWords: Int = args("num-words").toInt
    val outputFile: String = args("output-file")
  }

  def runPipeline(inputFile: String, numWords: Int, outputFile: String)(implicit sc: ScioContext): Unit = {
    val lines: SCollection[String] = sc.textFile(inputFile)  // ["En un lugar de La Mancha", ...
    val words: SCollection[String] = lines.flatMap(_.split(" "))  // ["En", "un", "lugar"...
    val cleanWords: SCollection[String] = words.map(sanitizeWord)  // ["en", "un", "lugar"...
    val counted: SCollection[(String, Long)] = cleanWords.countByValue
    val topWords: SCollection[Iterable[(Long, String)]] = counted.swap.top(numWords)
    val csvLines: SCollection[Iterable[String]] = topWords.map { iter =>
      iter.map { case (num: Long, word: String) =>
        List(num.toString, word).mkString(",")
      }
    }
    val csvContent: SCollection[String] = csvLines.map(_.mkString("\n"))
    csvContent.saveAsTextFile(outputFile)
  }

  def sanitizeWord(w: String): String = {
    w.toLowerCase
      .filter(c => !thingsToRemove.contains(c))
      .map(c => swapMap.getOrElse(c, c))
  }
}
