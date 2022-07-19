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

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    val inputFile: String = args("input-file")
    val numWords: Int = args("num-words").toInt
    val outputFile: String = args("output-file")

    runPipeline(inputFile, numWords, outputFile)
  }

  def runPipeline(inputFile: String, numWords: Int, outputFile: String)(implicit sc: ScioContext): Unit = {
    ???

    sc.run()
  }
}
