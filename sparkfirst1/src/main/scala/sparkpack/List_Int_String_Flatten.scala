package sparkpack

object sparkobj {

  def main(args: Array[String]): Unit = {

    println("started")

    val a = 2;
    println(a)

    val b = "zeyo";
    println(b)

    val listInt = List(1, 2, 3, 4, 5, 6, 9, 12);
    println("========  raw list==============")
    listInt.foreach(println)
    println(listInt)

    println("======================================")

    /*Iterate each element of the list and filter number >5 and
    save it into new list*/

    val iterate = listInt.filter(x => x > 5)

    println("========  filter list==============")
    iterate.foreach(println)

    val iterate1 = listInt.filter(x => x == 5)

    println("========  filter list==============")
    iterate1.foreach(println)

    val listIn = List(1, 2, 3, 4);

    println("========  raw list==============")
    listInt.foreach(println)
    println(listInt)

    println("========  Map list==============")

    val mulList = listIn.map(x => x * 2);
    mulList.foreach(println)

    println("========  Map list==============")

    val mullist = listIn.map(x => x + 10)
    mullist.foreach(println)

    println("=========List of String ===============================================================================")
    println()

    val listString = List("vicky", "vineeta", "lucky", "sonali")
    listString.foreach(println)

    listString.filter(x => x.contains("lucky")).foreach(println)

    val iterateString = listString.filter(x => x.equals("vineeta"))
    iterateString.foreach(println)

    println()
    val addName = listString.map(x => x + " + vicky").foreach(println)
    // OR
    println()
    val name = ", vicky"
    val addName1 = listString.map(y => y + name).foreach(println)
    println()

    println("============== Task Started====================================")

    val lis = List("zeyobron", "zeyo", "azeyobron", "analytics")
    val iterating = lis.filter(x => x.contains("zeyo"))
    iterating.foreach(println)

    println("============== Task Ended====================================")
    println
    println("============== Task Started====================================")

    val listStateCity = List("State->Madhya", "City->Indore", "State->Maharastra", "City->Pune")

    val filterState = listStateCity.filter(x => x.contains("State->"))
    filterState.foreach(println)

    val filterCity = listStateCity.filter(x => x.contains("City->"))
    filterCity.foreach(println)

    println("============== Task Ended====================================")

    println
    println("============== Task Started====================================")

    val list = List("A~B", "C~D", "E~F~G")
    list.foreach(println)
    val flattenList = list.flatMap(x => x.split("~"))
    flattenList.foreach(println)
    
    println
    
     val list1 = List("A,B", "C,D", "E,p,o,f,F,G")
    list.foreach(println)
    val flattenList1 = list1.flatMap(x => x.split(","))
    flattenList1.foreach(println)
    println("============== Task Ended====================================")

    println
    println("============== Task Started====================================")

    val listStr = List("A~B", "C~D", "E~F")
    val flattenListt = listStr.flatMap(z => z.split("~"))
    val filterList = flattenListt.filter(x => x.contains("B"))
    val replace = filterList.map(x => x.replace("B", "Vicky"))
    val suffix = replace.map(x => x.concat(" patil"))
    suffix.foreach(println)

    println("============== Task Ended====================================")
    println
    println("============== Task Started====================================")
    
    val listStateCityy = List("State->Madhya", "City->Indore", "State->Maharastra", "City->Pune")

    val filterState1 = listStateCityy.filter(x => x.contains("State"))
    val replaceState=filterState1.map(x=>x.replace("State->", ""))
    replaceState.foreach(println)
    val filterCity1 = listStateCityy.filter(x => x.contains("City"))
    val replaceCity=filterCity1.map(x=>x.replace("City->", ""))
    replaceCity.foreach(println)
    
    println("============== Task Ended====================================")
    println
    println
    
   
    
     val data=List("Gymnastics","Cash");
     val filterGym=data.filter(x=>x.contains("Gymnastics"))
        filterGym.foreach(println)
    
    
    
  }
}