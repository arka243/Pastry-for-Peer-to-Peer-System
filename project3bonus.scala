import akka.actor._
import scala.collection.mutable._
import akka.actor.Actor._
import java.security.MessageDigest
import java.util.UUID
import scala.math._
import java.lang.Long
import scala.util.Random
import scala.collection.immutable.TreeMap


object project3bonus {
  
  var flag = 0
  var wantfailure=false
  
  val system =ActorSystem("PastryBonus")
  
  case class Start(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Initial(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class JoinNetwork(ID:String,path:List[String],NetWork:HashMap[String, ActorRef])
  case class Message(index:Int,from:String,nodeID:String,msgkey:String,path:String,hopnum:Int,NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class sendState(nodeID:String,path:List[String],leftleaf:Array[String],rightleaf:Array[String],T:Array[Array[String]])
  case class SendMsg(network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class MsgArrival(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int)
  case class setMode(a:Int,b:List[(String,Int)])
  case class Contact(seq:Int,from:String,init:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Failure(cat:String,seq:Int,from:String,Start:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class FCon(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String],wakeup:Int)
  case class FCon2(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class FailureConfigure(id1:String,id2:String,fflag:Int)
  case class Wakeup(from:String)
  case class End(indexID:Int,totalhops:Int)
  
  def main(args: Array[String]): Unit = {
    
    while(flag!=1)
    {
      println("Enter the Number of Nodes, Number of Requests and If you want a fail module")
      var inputstring = Console.readLine()
      val inputarr = inputstring.split(" ")
      if(inputarr.length < 2 || inputarr.length > 4)
      {
        println("Invalid Entries! Please Try Again!")
      }
      else if(inputarr.length == 3 && inputarr(2).toLowerCase() == "yes")
      {
        flag = 1
        wantfailure = true
        val numNodes = inputarr(0).toInt
        val numRequests = inputarr(1).toInt
        var master=system.actorOf(Props(new Master(numNodes,numRequests, wantfailure)))
      }
      else
      {
        flag = 1
        val numNodes = inputarr(0).toInt
        val numRequests = inputarr(1).toInt
        var master=system.actorOf(Props(new Master(numNodes,numRequests, wantfailure)))
      }
    }
  }
  
  def md(num:Long): String ={
	 val leng = bits/b
	 val digit = Long.toString(num,math.pow(2,b).toInt)
	 var spl = ""
	 for(i <- 0 until leng-digit.length){
	   spl = spl+"0"
	 }
	 spl+digit
  }

  def getnetworkIp(network: HashMap[String,ActorRef]): String = {
    val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
    if (!network.contains(ip)) ip else getnetworkIp(network)
  }
  
val bits=32
val b=4
val L=math.pow(2,b).toInt
class Node(nodeindex:Int, ID: String,numRequests:Int,master:ActorRef) extends Actor {

   val Master=master
   val T=new Array[Array[String]](bits/b)
   val leftleaf=new Array[String](L/2)
   val rightleaf=new Array[String](L/2)
   val indexID=nodeindex
   val nodeID=ID
   var ArrivalCount, totalhops, diestate=0
   val retransmit = 3
   var statemap= HashMap.empty[String,Int] 
   val failconns = HashMap.empty[String,Int]
   
   def Initialize(network:HashMap[String, ActorRef],nodemap:TreeMap[Int,String])={
     val rtcolumn=math.pow(2,b).toInt
     for(i<-0 until T.size){
       T(i)=new Array[String](rtcolumn)
     }
     val Sortnode=nodemap.values.toArray
     util.Sorting.quickSort(Sortnode)
     val index=Sortnode.indexOf(nodeID) 
     for(i<-0 until L/2){
       
        leftleaf(i)={if(index>=L/2-i)Sortnode(index-L/2+i)else Sortnode(0)}
        rightleaf(i)={if(index<Sortnode.size-i-1)Sortnode(index+i+1)else Sortnode.last}
     }
  
     for(i<-0 until T.size){
         var prefixR =nodeID.substring(0, i)
         var digitR=nodeID.substring(i, i+1)
         var IntdigitR=Integer.parseInt(digitR,16)
         for(j<-0 until rtcolumn){
             if(j==IntdigitR){T(i)(j)=nodeID}
             else if(j<IntdigitR){T(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,0,index)}
             else{
               if (index+1<Sortnode.size){T(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,index+1,Sortnode.size)}
             }          
         }
         
     }
        leftleaf.foreach(x => if(x!=nodeID) statemap+={x->0})
	    rightleaf.foreach(x => if(x!=nodeID) statemap+={x->0})
	   
	    T.foreach( x => x.foreach(y => if(y!=nodeID&&y!=null) statemap+={y->0}) )
	    
     self! SendMsg(network,nodemap)
   }
   	
   def setMode(state:Int,conns:List[(String,Int)]) = {
	    
	    diestate = state
	    if(conns!=null && conns.size>0){
	      conns.foreach( pair => failconns+= {pair._1 -> pair._2})
	    }
	  }
	  
   def findID(prefixR:String, Sortnode:Array[String],beginN:Int,endN:Int):String={
     for(m<-beginN until endN){
       if(Sortnode(m).startsWith(prefixR)) {return Sortnode(m)}       
     }
     return null
   }
   
   def alter(k:String, orig:String, curr:String,nodemap:TreeMap[Int,String]):String = {
	    
	    val sortN = nodemap.values.toArray
	    util.Sorting.quickSort(sortN)
	    val index = sortN.indexOf(orig)
	    
	    var result = orig
	    var resindex = index
	    if(index==0 || (k>orig && index<sortN.length-1)) {result=sortN(index+1);resindex=index+1}
		    else { result=sortN(index-1);resindex=index-1 }
		    
	    while(result==curr){
		  resindex += resindex-index
		  if(resindex==sortN.size){resindex=index-1}
		  else if(resindex==0){ resindex = index+1}
		  result = sortN(resindex)
	    }
	    
	    return result
	    
	  }
	  
   
   def getroute(thekey:String):String = { 
           
     if (thekey==null ||leftleaf(0)==null||rightleaf(0)==null||leftleaf.last==null||rightleaf.last==null){return nodeID}

       var cha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(nodeID, 16))
       var Mindex= -1
       
       if(thekey>=leftleaf(0) && thekey<=nodeID){ 
            for(i<- 0 until leftleaf.size){
               val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(leftleaf(i), 16))
               if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i
                      
                 }
            }
         if (Mindex<0) return nodeID else return leftleaf(Mindex)
       }
       else if(thekey > nodeID && thekey <= rightleaf(rightleaf.size-1)){
           for (i<- 0 until rightleaf.size){
                val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(rightleaf(i), 16))
                if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i                      
                 } 
           }
           if (Mindex<0) return nodeID else return rightleaf(Mindex) 
       }
       else{  
         var mcp=(thekey,nodeID).zipped.takeWhile(Function.tupled(_==_)).map(_._1).mkString
         var lenOfmcp=mcp.length
         var digitR=Integer.parseInt(thekey.substring(lenOfmcp, lenOfmcp+1), 16) 
         if (T(lenOfmcp)(digitR)!=null){
             return T(lenOfmcp)(digitR)
         }
         else{
           val row=T(lenOfmcp) 
           for(i<-0 until row.size){
               val tempcha=if (row(i)==null) cha else math.abs(Long.parseLong(thekey, 16)-Long.parseLong(row(i), 16))
               if (tempcha<cha){
                  cha=tempcha
                  Mindex=i
               }
           }
           if(Mindex>=0){return row(Mindex)}
           else if(thekey<nodeID) return leftleaf(0)
           else return rightleaf.last
         }         
       }     
   }
   
   def receive ={
    case Start(network,nodemap)=>Initialize(network,nodemap)
  
    case Initial(network,nodemap)=>{            
                  if(indexID!=0){
                         var AID=nodemap(indexID-1)
                         network(AID)! JoinNetwork(nodeID,List(nodeID),network)                    
                                }
                     }
    case JoinNetwork(initalnode,path,network) => {
                 if (Long.parseLong(initalnode, 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(leftleaf)
                       if (Long.parseLong(initalnode, 16)>Long.parseLong(leftleaf(0), 16)){leftleaf(0)=initalnode}
                 }
                 else if (Long.parseLong(initalnode, 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rightleaf)
                       if (Long.parseLong(initalnode, 16)<Long.parseLong(rightleaf.last, 16)){rightleaf(L/2-1)=initalnode}
                 }     

                 val routenode=getroute(initalnode)
                 if (routenode!=nodeID){
                   network(routenode)! JoinNetwork(initalnode,path++List(nodeID),network)
                   network(initalnode)! sendState(nodeID,path++List(nodeID),leftleaf,rightleaf,T)
                 }
                 else {
                   network(initalnode)! sendState(nodeID,path++List(nodeID),leftleaf,rightleaf,T)
                   network(initalnode)! "lastJoinNetwork"
                   
                 } 
      
    }
    case sendState(pathnode,path,oleftleaf,orightleaf,oT)=>{
      var lenOfpath=path.size
      T(lenOfpath-1)=oT(lenOfpath-1)  
      if (Long.parseLong(path.last, 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(leftleaf)
                       if (Long.parseLong(path.last, 16)>Long.parseLong(leftleaf(0), 16)){leftleaf(0)=path.last}
                 }
                 else if (Long.parseLong(path.last, 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rightleaf)
                       if (Long.parseLong(path.last, 16)<Long.parseLong(rightleaf.last, 16)){rightleaf(L/2-1)=path.last}
                 } 
      
    }
   
    case FCon(fnetwork,fmymap,wakeup)=>{
      val conleaf = leftleaf++rightleaf
      var id1=nodeID
      val id2 = conleaf((math.random*conleaf.size).toInt)
      val conns1 = List[(String,Int)]((id2,wakeup)); setMode(1, conns1)
      val conns2 = List[(String,Int)]((id1,wakeup)); fnetwork(id2)!setMode(1, conns2)
      master! FailureConfigure(id1,id2,1)
      println(" Connection dies temporarily: Node (" + id1 + ")<->N (" + id2 + ")")
    }
   
    case setMode(a,b)=>setMode(a,b)
    
    case FCon2(fnetwork,fmymap)=>{
      val conleaf = leftleaf++rightleaf
      var idp1=nodeID
      val idp2 = conleaf((math.random*conleaf.size).toInt)
      val connsp1 = List[(String,Int)]((idp2,-1)); setMode(1, connsp1)
      val connsp2 = List[(String,Int)]((idp1,-1)); fnetwork(idp2)!setMode(1, connsp2)
      master! FailureConfigure(idp1,idp2,2)
      println("  (" + idp1 + ") <-> (" + idp2 + ")  connection dies permanently")
    }
    case SendMsg(network,nodemap)=>{ 
               for(i<-1 to numRequests) {
               var msgkey=md((math.random*mymax).toLong)
               val routenode=getroute(msgkey)
               if (routenode==nodeID){
                 network(routenode)! MsgArrival(i,nodeID,msgkey,"null",0)
               }
               else {network(routenode)! Message(i,nodeID,nodeID,msgkey,"",1,network,nodemap)}
          } 
      
    }
      
    case Message(j,from,initnode,msgkey,path,hops,network,nodemap)=>{
  
               
	          if(diestate == 2 && (!statemap.contains(from) || statemap(from)>=0)){
	            network(from) ! Failure("Messg",j,nodeID,initnode,msgkey,path,hops,network,nodemap)
	          }
	          else if(diestate>0 && failconns.contains(from) && failconns(from)!=0){
	            network(from) ! Failure("Messg",j,nodeID,initnode,msgkey,path,hops,network,nodemap)
	            failconns +={from->(failconns(from)-1)}	            
	          }
	          else{	              
	              if(diestate>0 && failconns.contains(from) && failconns(from)==0){
		              failconns.remove(from)
		              if(diestate==1 && (failconns.size==0||failconns(from)==0)) { diestate=0 }
		              network(from) ! Wakeup(nodeID)
	              }
	            
	           val routenode=getroute(msgkey)
               if (routenode==nodeID){
                   network(initnode)! MsgArrival(j,initnode,msgkey,path+nodeID,hops)
               }
               else {network(routenode)! Message(j,nodeID,initnode,msgkey,path+nodeID+"+",hops+1,network,nodemap)} 

	          }

    }
    case MsgArrival(j,initnode,msgkey,path,hops)=>{
           ArrivalCount+=1
           totalhops+=hops
           if(j==1)
				println("node "+nodeID+" receive the "+j+"st message, hop is "+hops)
			else if(j==2)
				println("node "+nodeID+" receive the "+j+"nd message, hop is "+hops)
			else if(j==3)
				println("node "+nodeID+" receive the "+j+"rd message, hop is "+hops)
			else
				println("node "+nodeID+" receive the "+j+"th message, hop is "+hops)
			if(ArrivalCount==numRequests){
               master! End(indexID,totalhops)
           }
                     
    }
    case "stop"=>System.exit(1)
    case Failure(cat,seq,from,initnode,key,path,hops,network,nodemap) => {
	          
	          if(statemap(from)<retransmit){
	              
	              println( "(" + nodeID + ") temporarily fails connectted with (" + from + ") " )
	            
		          statemap += {from->(statemap(from)+1)}

		          network(from) ! Message(seq,nodeID,initnode,key,path+nodeID+"+",hops+1,network,nodemap)
	          }
	          else{
	            var next = alter(key,from,nodeID,nodemap)
	            
	            println( "(" + nodeID + ") permanently fails connectted with (" + from + ") !" +"\n"
	                + "use (" +next + ") replace " + from  )
	            
	            network(from) ! Contact(seq,nodeID,initnode,key,path,hops,network,nodemap)
	            
	          }
	}
	        
  case Wakeup(from) => {
	          println("N" + indexID + "(" + nodeID + ") find connect with N"  
	                + "(" + from + ") wake up!")
	        }
 case Contact(seq,from,initnode,msgkey,path,hops,network,nodemap) => {
	          if(diestate>0 && failconns.contains(from) && failconns(from)<0){
	            failconns.remove(from) 
	            if(diestate==1 && failconns.size==0) { diestate=0 }
	          }
	          else if(diestate==2){
	              
	            statemap+=(from -> (-1))
	              
	          }
	          val routenode=getroute(msgkey)
               if (routenode==nodeID){
                   network(initnode)! MsgArrival(seq,initnode,msgkey,path+nodeID,hops)
               }
               else {network(routenode)! Message(seq,nodeID,initnode,msgkey,path+nodeID+"+",hops+1,network,nodemap)} 
	          	          	          
	        }
    case _=>{}
  } 

}
 val mymax=math.pow(2,bits).toLong
class Master(numNodes: Int, numRequests:Int,wantfailure:Boolean) extends Actor with ActorLogging {
    var count = 0
    var totalhops=0
     var NetWork = HashMap.empty[String, ActorRef] 
    var mymap = new TreeMap[Int,String]
   
  

    override def preStart() {
        for (i <- 0 until numNodes){
                var IP=getnetworkIp(NetWork)
                var ID=md((math.random*mymax).toLong)
                  mymap +={i->ID}
                
                var ss=i
                var node=system.actorOf(Props(new Node(ss,ID, numRequests, self)))
               NetWork +={ID->node}
                                
        }
        
     for(allnodes<-NetWork.keysIterator){NetWork(allnodes)! Start(NetWork,mymap)}   
     if(wantfailure){      
       		 val wakeup = 2
    			 val n1 = (math.random*mymap.size+1).toInt
    			 NetWork(mymap(n1)) ! FCon(NetWork,mymap,wakeup)
    	         var np1 = (math.random*mymap.size+1).toInt
                 while(np1==n1){ np1 = (math.random*mymap.size+1).toInt }
                 NetWork(mymap(np1)) ! FCon2(NetWork,mymap)
         }   
    }
    var failcount=0
    var id1="";var id2="";var id3="";var id4=""
   
      def receive = {
       case FailureConfigure(n1,n2,flag)=>{
         if (flag==1){var id1=n1; var id2=n2;failcount =failcount+1}
         if (flag==2){var id3=n1; var id42=n2;failcount =failcount+1}
         if (failcount==2){
             var failnd = (math.random*mymap.size+1).toInt
             var failndID = mymap(failnd)
             while(List(id1,id2,id3,id4).contains(failndID)){ failnd = (math.random*mymap.size+1).toInt; failndID= mymap(failnd)}
             NetWork(failndID)!setMode(2,null)
             println("Node dies: N" + failnd + "(" + failndID + ")\n")
         }
         
       }
       case End(nodeindex,nodehop) =>{
               totalhops+=nodehop
               count+=1
               if (count==numNodes){
                 println("the whole network's average hops is :"+(totalhops/(numRequests.toDouble*numNodes.toDouble)))
                 for(allnode<-NetWork.keysIterator){NetWork(allnode)!"stop"}
                 System.exit(1)
               }
               
         
       }
       case _ =>{}
  }
 }
}
