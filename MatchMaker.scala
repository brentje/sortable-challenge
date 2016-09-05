/*
******************************************************************************
*
* MatchMaker.scala 
* A solution to the Sortable.com coding challenge
*
* Author: Brent Englehart
*
* Originally built for Scala 2.9
*
* To Build
* --------
* To compile this program, run the Build command.  This only needs to be run once:
*	sh build.sh
*
* To Run
* ------	
* To run the compiled version of this program, type:
*	sh go.sh
*
* Alternatively, you can run this program without building it first by typing:
*	scala MatchMaker.scala
*
*
* This program will accept 2 JSON files as input, "products.txt" and "listings.txt"
* which are located in the "data" folder.  
*
* Input Files:
*
* product.txt
* {
* "product_name": String // A unique id for the product
* "manufacturer": String
* "family": String // optional grouping of products
* "model": String
* "announced-date": String // ISO-8601 formatted date string, e.g. 2011-04-28T19:00:00.000-05:00
* }
*
* listing.txt
* {
* "title": String // description of product for sale
* "manufacturer": String // who manufactures the product for sale
* "currency": String // currency code, e.g. USD, CAD, GBP, etc.
* "price": String // price, e.g. 19.99, 100.00
* }
*
* The files will be processed, and each record in products.txt will be 
* searched for within the listings.txt file.  Multiple records, if available,
* will be pulled from listings.txt in order to create search results.
* 
*
* Results File:
*
* A file named "results.txt", located in the "results" folder, will contain
* a list of all results found for each product record.  If a "results.txt"
* already exists, it will be overwritten.
*
* results.txt 
* {
* "product_name": String
* "listings": Array[Listing]
* }
*
******************************************************************************
*/

import scala.collection.mutable.ArrayBuffer
import scala.actors.threadpool.Executors
import scala.actors.threadpool.Callable

import scala.io.Source
import scala.util.parsing.json

import java.io._

object MatchMaker{
	def main(args: Array[String]){

		var listings: Array[String] = Array()
		var titleindex: Array[String] = Array()

		var manufacturerindex: Map[String, ArrayBuffer[Int]] = Map() 


		val start = System.nanoTime()
		
		indexManufacturers("listings.txt")
		println("Indexing Time: "+ ((System.nanoTime() - start )/ 1e9) + " seconds.")
		
		createResultsFromProducts("products.txt")
		println("Total Time: " + ((System.nanoTime() - start )/ 1e9) + " seconds.")


		//Create an index of all the manufacturers found in the listings, along with the title from each indexed record, to improve search speed.
		def indexManufacturers(filename: String){
			println("Creating index of Manufacturers")

			try {				
				listings = Source.fromFile("./data/" + filename).getLines().toArray
			} catch {
			  case ex: FileNotFoundException => println("Couldn't find that file: $filename")
			  case ex: IOException => println("Had an IOException trying to read that file")
			}

			var index : Int = 0
			var titleindexbuffer: ArrayBuffer[String] = new ArrayBuffer()

			for (currentlisting <- listings){
				try{
					//Clean the manufacturer and title to force capitalization and strip non-alphanumeric characters.
					var cleanmanufacturer = currentlisting.substring(currentlisting.indexOf("manufacturer") + 15, currentlisting.length).split("\",\"", 2)(0)
					var cleantitle = currentlisting.substring(currentlisting.indexOf("title") + 8, currentlisting.length).split("\",\"", 2)(0)

					//Just take the first word of the manufacturer, to strip out unnecessary information 
					cleanmanufacturer = cleanString(cleanmanufacturer).split(" ", 2)(0)
					cleantitle = cleanString(cleantitle) 

					if (cleanmanufacturer.isEmpty){
						//Manufacturer was empty or contained invalid data.  
						//Take the first word from the title to use as the manufacture.
						//This should cover most instances as listings usually state the manufacturer in the title first.
						cleanmanufacturer = cleantitle.split(" ", 2)(0)
					}

					//Check if the current manufacturer has already been added to the index.
					if (manufacturerindex.contains(cleanmanufacturer)){
						manufacturerindex(cleanmanufacturer) += index
					}else{
						manufacturerindex += (cleanmanufacturer -> ArrayBuffer[Int](index))
					}

					titleindexbuffer += cleantitle
				} catch {
				  case ex: Exception => println("Error processing manufacturers: " + currentlisting)
				}

				//Whether the listing line processed properly or not, increment the index to stay in sequence with the file.
				index += 1			
			}

			titleindex = titleindexbuffer.toArray

			var titlemap: Map[String, ArrayBuffer[Int]] = Map() 
			//Manufacturers may be mentioned within the title field, notably in cases of 3rd party suppliers.
			//Search through all manufacturers to find other instances of the current manufacturer.
			for (currentmanufacturer <- manufacturerindex.keySet)  {
				for (manufacturer <- manufacturerindex.keySet) {
					//loop through all the manufacturers looking for anything but the current manufacturer
					if (manufacturer != currentmanufacturer) {
						for (index <- manufacturerindex(manufacturer)){
							//Loop through all titles of this manufacturer to search for the current manufacturer
							if (titleindex(index).contains(currentmanufacturer)){
								//Check if the current manufacturer has already been added to the temporary index.
								if(!manufacturerindex(currentmanufacturer).contains(index)) {
									if(titlemap.contains(currentmanufacturer)) {
										titlemap(currentmanufacturer) += index
									}else{
										titlemap += (currentmanufacturer -> ArrayBuffer[Int](index))
									}
								}
							}
						}
					}
				}
			}

			//Add resulting index to the main index
			for(manufacturer <- titlemap.keySet){
				manufacturerindex(manufacturer) ++= titlemap(manufacturer)
			}
		}

		//Main function for getting results.
		//Load the products files, parse each line, and then process the search for the product.
		def createResultsFromProducts(filename: String){
			println("Parsing products.")
	
			val resultsdir = new File("./results")
			if(!resultsdir.exists) resultsdir.mkdir() 

			val resultsfile = new File("./results/results.txt")
			val bw = new BufferedWriter(new FileWriter(resultsfile))

			var products: List[String] = List()
			
			//Load the products file
			try {
				products = Source.fromFile("./data/" + filename).getLines().toList
			} catch {
			  case ex: FileNotFoundException => println("Couldn't find that file: " + filename)
			  case ex: IOException => println("Had an IOException trying to read that file")
			}
			
			//Create threads for each product record to improve processing speed.  Speed will increase based on number of CPU cores available.
			val es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())
			val futures = for (currentproduct <- products) yield {
				es.submit(new Callable {
					def call:String = {
						
						var productname = ""
						var manufacturer = ""
						var model = ""
						var family = ""
						
						//Parse the JSON line into it's individual fields.
						try{
							if(currentproduct.contains("product_name")){
								productname = currentproduct.substring(currentproduct.indexOf("product_name") + 15, currentproduct.length).split("\",\"", 2)(0)
							}else{
								println("Missing product name, ignoring: " + currentproduct)
						  		return "{\"product_name\":\"MISSING\", \"listings\":[]}\n"
							}

							if(currentproduct.contains("manufacturer")){
								manufacturer = currentproduct.substring(currentproduct.indexOf("manufacturer") + 15, currentproduct.length).split("\",\"", 2)(0)
							}else{
								println("Missing manufacturer, ignoring: " + currentproduct)
						  		return "{\"product_name\":\"" + productname + "\", \"listings\":[]}\n"
							}

							if(currentproduct.contains("model")){
								model = currentproduct.substring(currentproduct.indexOf("model") + 8, currentproduct.length).split("\",\"", 2)(0)
							}else{
								println("Missing model, ignoring: " + currentproduct)
						  		return "{\"product_name\":\"" + productname + "\", \"listings\":[]}\n"
							}

							//Family name is optional.  
							if(currentproduct.contains("family")){
								family = currentproduct.substring(currentproduct.indexOf("family") + 9, currentproduct.length).split("\",\"", 2)(0)
							}

							return getMatches(productname, manufacturer, model, family)
						} catch {
							//If there was a problem parsing the product, ignore and move on.
							case ex: Exception => println("Error processing product: " + currentproduct)
							return "{\"product_name\":\"MISSING\", \"listings\":[]}\n"
						}
					}
				})
			}

			//Pull the thread results in sequence and write the results to the results file.
			futures.foreach(f => (bw.write(f.get.toString)))
			
			es.shutdown()
			bw.close()

		}
				
		//Create a string containing the JSON results of our search
		def getMatches(productname : String, manufacturer : String, model : String, family : String = ""): String = {
			//All indexed information is stored clean.  Clean input variables to match.
			var cleanmanufacturer = cleanString(manufacturer).split(" ", 2)(0)
			var cleanmodel = cleanString(model)
			var cleanfamily = cleanString(family)
				
			var matches = ""
		
			//Search for the product model within the index.
			if (manufacturerindex.contains(cleanmanufacturer)){
				var extraresultscount = 0

				for (index <- manufacturerindex(cleanmanufacturer)) {
					if (titleindex(index).contains(cleanmodel)){
						//Positive hit.  We found the model within a listing that also contained the manufacturer.
						matches += listings(index) + ","
					}			
				}

				//Strip the last comma from the results string
				if (!matches.isEmpty){
					matches = matches.split(".$")(0)
				}

			}
	
			return "{\"product_name\":\"" + productname + "\", \"listings\":[" + matches + "]}\n"				
		}		
		
		//Clean strings to force capitalization and strip non-alphanumeric characters.
		def cleanString(inputstring : String): String = {
			return inputstring.toUpperCase().replaceAll("[^A-Z0-9 ]", "")
		}		
	}
}
