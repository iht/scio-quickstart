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
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection

object ScioQuickstartPipeline {

  val thingsToRemove: List[Char] =
    List('.', ',', '?', '!', '¿', ';')

  val accents: Map[Char, Char] = Map(
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

    runPipeline(inputFile, numWords, outputFile)
  }

  def sanitizeWord(w: String): String = {
    w.toLowerCase
      .filter(c => !thingsToRemove.contains(c))
      .map(c => accents.getOrElse(c, c))
  }


  def runPipeline(inputFile: String, numWords: Int, outputFile: String)(implicit sc: ScioContext): Unit = {
    val lines: SCollection[String] = sc.textFile(inputFile)
    // ["En un lugar de La Mancha...", "de cuyo nombre no quiero..."]
    val words: SCollection[String] = lines.flatMap(_.split(" "))
    // ["En", "un", "lugar", ...]
    val clean: SCollection[String] = words.map(sanitizeWord)
    // ["en", "un", "lugar", ...]
    val counted: SCollection[(String, Long)] = clean.countByValue
    val swapped: SCollection[(Long, String)] = counted.swap
    val topWords: SCollection[Iterable[(Long, String)]] = swapped.top(numWords)
    val csvLines: SCollection[String] = topWords.map {
      t: Iterable[(Long, String)] =>
        val csvLines: Iterable[String] = t.map { case (n: Long, w: String) =>
          List(n.toString, w).mkString(",")  // "7,mancha"
        }
        csvLines.mkString("\n")
    }
    csvLines.saveAsTextFile(outputFile)

    sc.run()
  }
}
