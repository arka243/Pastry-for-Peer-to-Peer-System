import akka.actor._
import scala.collection.mutable._
import akka.actor.Actor._
import java.security.MessageDigest
import java.util.UUID
import scala.math._
import java.lang.Long
import scala.util.Random
import scala.collection.immutable.TreeMap


object project3 
{
  var flag = 0

  val system =ActorSystem("Pastry")
  
  case class Start(network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Initial(network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Joinnetwork(ID:String,path:List[String],network:HashMap[String, ActorRef])
  case class Message(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int,network:HashMap[String, ActorRef])
  case class sendState(nodeID:String,path:List[String],leftleaf:Array[String],rightleaf:Array[String],T:Array[Array[String]])
  case class SendMsg(network:HashMap[String, ActorRef])
  case class MsgArrival(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int)
  case class LastJoin(leftleaf:Array[String],rightleaf:Array[String],network:HashMap[String, ActorRef])
  case class End(indexID:Int,totalhop:Int)
  
  def main(args: Array[String]): Unit = {
    
    while(flag!=1)
    {
      println("Enter the Number of Nodes & Number of Requests")
      var inputstring = Console.readLine()
      val inputarr = inputstring.split(" ")
      if(inputarr.length != 2)
      {
        println("Invalid Entries! Please Try Again!")
      }
      else
      {
        flag = 1
        val numNodes = inputarr(0).toInt
        val numRequests = inputarr(1).toInt
        var master=system.actorOf(Props(new Master(numNodes,numRequests)))
      }
    }
  }

  def md(num:Long): String =
  {
    val leng = bits/b
	val digit = Long.toString(num,math.pow(2,b).toInt)
	var spl = ""
	for(i <- 0 until leng-digit.length)
	{
	  spl = spl+"0"
	}
	 spl+digit
  }
  
  def getnetworkIp(network: HashMap[String,ActorRef]): String = 
  {
    val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
    if (!network.contains(ip)) ip else getnetworkIp(network)
  }
  
  
  val bits=32
  val b=4
  val L=math.pow(2,b).toInt
  
  class Node(nodeindex:Int, ID: String,numRequests:Int,master:ActorRef) extends Actor 
  {

    val Master=master
    val T=new Array[Array[String]](bits/b)
    val leftleaf=new Array[String](L/2)
    val rightleaf=new Array[String](L/2)
    val indexID=nodeindex
    val nodeID=ID
    var ArrivalCount, totalhop=0
   
    def Initialize(network:HashMap[String, ActorRef],nodemap:TreeMap[Int,String])=
	{
      val rtcolumn=math.pow(2,b).toInt
      for(i<-0 until T.size){
        T(i)=new Array[String](rtcolumn)
      }
      val Sort=nodemap.values.toArray
      util.Sorting.quickSort(Sort)
      val index=Sort.indexOf(nodeID) 
      for(i<-0 until L/2)
	  {
        leftleaf(i)={if(index>=L/2-i)Sort(index-L/2+i)else Sort(0)}
        rightleaf(i)={if(index<Sort.size-i-1)Sort(index+i+1)else Sort.last}
      }
      for(i<-0 until T.size)
	  {
        var prefix =nodeID.substring(0, i)
        var digit=nodeID.substring(i, i+1)
        var Intdigit=Integer.parseInt(digit,16)
        for(j<-0 until rtcolumn)
		{
            if(j==Intdigit)
			{
			  T(i)(j)=nodeID
			}
            else if(j<Intdigit)
			{
			  T(i)(j)=findID(prefix+Integer.toString(j,16),Sort,0,index)
			}
            else
			{
              if (index+1<Sort.size){T(i)(j)=findID(prefix+Integer.toString(j,16),Sort,index+1,Sort.size)}
            }          
        }        
      }
      self! SendMsg(network)
    }
	
    def findID(prefix:String, Sort:Array[String],begin:Int,end:Int):String=
	{
      for(m<-begin until end){
        if(Sort(m).startsWith(prefix)) 
		{
		  return Sort(m)
		}       
      }
      return null
    }
    
	def getroute(key:String):String = 
	{
      if (key==null ||leftleaf(0)==null||rightleaf(0)==null||leftleaf.last==null||rightleaf.last==null)
	  {
	    return nodeID
      }
      var ch=math.abs(Long.parseLong(key, 16)-Long.parseLong(nodeID, 16))
      var Mdx= -1
      if(Long.parseLong(key, 16)>=Long.parseLong(leftleaf(0), 16) && Long.parseLong(key, 16)<=Long.parseLong(nodeID, 16))
	  {
        for(i<- 0 until leftleaf.size)
		{
            val temp=math.abs(Long.parseLong(key, 16)-Long.parseLong(leftleaf(i), 16))
            if (temp<=ch)
			{
                ch=temp
                Mdx=i
            }
        }
        if (Mdx<0) 
			return nodeID 
		else 
			return leftleaf(Mdx)
      }
      else if(key > nodeID && key <= rightleaf(rightleaf.size-1))
	  {
        for (i<- 0 until rightleaf.size)
		{
            val temp=math.abs(Long.parseLong(key, 16)-Long.parseLong(rightleaf(i), 16))
            if (temp<=ch)
			{
                ch=temp
                Mdx=i                      
            } 
        }
        if (Mdx<0) 
			return nodeID 
		else 
			return rightleaf(Mdx) 
      }
      else
	  {
        var m=(key,nodeID).zipped.takeWhile(Function.tupled(_==_)).map(_._1).mkString
        var lengthm=m.length
        var digit=Integer.parseInt(key.substring(lengthm, lengthm+1), 16)
        if (T(lengthm)(digit)!=null)
		{
            return T(lengthm)(digit)
        }
        else
		{
            val row=T(lengthm) 
            for(i<-0 until row.size)
			{
			    val temp = if(row(i)==null) ch else math.abs(Long.parseLong(key, 16)-Long.parseLong(row(i), 16))
				if (temp<ch)
				{
				  ch=temp
                  Mdx=i
				}
			}
			if(Mdx>=0)
			{
				return row(Mdx)
			}
			else if(key<nodeID) 
				return leftleaf(0)
			else 
				return rightleaf.last
        }         
      }     
	}
   
	def receive =
	{
		case Start(network,nodemap)=>
			Initialize(network,nodemap) 
        
		case Initial(network,nodemap)=>
		{            
            if(indexID!=0)
			{
                var AID=nodemap(indexID-1)
				network(AID)! Joinnetwork(nodeID,List(nodeID),network)                    
            }
        }
		
		case Joinnetwork(initalnode,path,network) => 
		{
			if (Long.parseLong(initalnode, 16)<Long.parseLong(nodeID, 16))
			{
                util.Sorting.quickSort(leftleaf)
                if (Long.parseLong(initalnode, 16)>Long.parseLong(leftleaf(0), 16)){leftleaf(0)=initalnode}
            }
            else if (Long.parseLong(initalnode, 16)>Long.parseLong(nodeID, 16))
			{
                util.Sorting.quickSort(rightleaf)
                if (Long.parseLong(initalnode, 16)<Long.parseLong(rightleaf.last, 16)){rightleaf(L/2-1)=initalnode}
            }     
			val routenode=getroute(initalnode)
            if (routenode!=nodeID)
			{
                network(routenode)! Joinnetwork(initalnode,path++List(nodeID),network)
                network(initalnode)! sendState(nodeID,path++List(nodeID),leftleaf,rightleaf,T)
            }
            else 
			{
                network(initalnode)! sendState(nodeID,path++List(nodeID),leftleaf,rightleaf,T)
                network(initalnode)! LastJoin(leftleaf,rightleaf,network)
            }
        }
		
		case sendState(pathnode,path,lleaf,rleaf,t)=>
		{
			var lenOfpath=path.size
			T(lenOfpath-1)=t(lenOfpath-1)  
			if (Long.parseLong(path.last, 16)<Long.parseLong(nodeID, 16))
			{
				util.Sorting.quickSort(leftleaf)
				if (Long.parseLong(path.last, 16)>Long.parseLong(leftleaf(0), 16))
				{
					leftleaf(0)=path.last
				}
			}
			else if (Long.parseLong(path.last, 16)>Long.parseLong(nodeID, 16))
			{
				util.Sorting.quickSort(rightleaf)
				if (Long.parseLong(path.last, 16)<Long.parseLong(rightleaf.last, 16))
				{
					rightleaf(L/2-1)=path.last
				}
			} 
		}
    
		case LastJoin(lleaf,rleaf,network)=> 
		{
			for(i<-0 until leftleaf.size)
			{
				if (Long.parseLong(lleaf(i), 16)<Long.parseLong(nodeID, 16))
				{
					util.Sorting.quickSort(leftleaf)
					if (Long.parseLong(lleaf(i), 16)>Long.parseLong(leftleaf(0), 16))
					{
						leftleaf(0)=lleaf(i)
					}
				}
				else if (Long.parseLong(lleaf(i), 16)>Long.parseLong(nodeID, 16))
				{
					util.Sorting.quickSort(rightleaf)
					if (Long.parseLong(lleaf(i), 16)<Long.parseLong(rightleaf.last, 16))
					{	
						rightleaf(L/2-1)=lleaf(i)
					}
				} 
				if (Long.parseLong(rleaf(i), 16)<Long.parseLong(nodeID, 16))
				{
					util.Sorting.quickSort(leftleaf)
					if (Long.parseLong(rleaf(i), 16)>Long.parseLong(leftleaf(0), 16))
					{	
						leftleaf(0)=rleaf(i)
					}
				}
				else if (Long.parseLong(rleaf(i), 16)>Long.parseLong(nodeID, 16))
				{
					util.Sorting.quickSort(rightleaf)
					if (Long.parseLong(rleaf(i), 16)<Long.parseLong(rightleaf.last, 16))
					{
						rightleaf(L/2-1)=rleaf(i)
					}
				} 
			}
			self!SendMsg(network)
		}
		
		case SendMsg(network)=>
		{
		    for(i<-1 to numRequests) 
			{
				var msgkey=md((math.random*mymax).toLong)
				val routenode=getroute(msgkey)
				if (routenode==nodeID)
				{
					network(routenode)! MsgArrival(i,nodeID,msgkey,"null",0)
				}
				else 
				{
					network(routenode)! Message(i,nodeID,msgkey,"",1,network)
				}
			}		 
		}
      
		case Message(j,initnode,msgkey,path,hops,network)=>
		{
            val routenode=getroute(msgkey)
            if (routenode==nodeID)
			{
				network(initnode)! MsgArrival(j,initnode,msgkey,path+nodeID,hops)
            }
			else 
			{
				network(routenode)! Message(j,initnode,msgkey,path+nodeID+"+",hops+1,network)
			}               
		}
		
		case MsgArrival(j,initnode,msgkey,path,hops)=>
		{
			ArrivalCount+=1
			totalhop+=hops
			if(j==1)
				println("node "+nodeID+" receive the "+j+"st message, hop is "+hops)
			else if(j==2)
				println("node "+nodeID+" receive the "+j+"nd message, hop is "+hops)
			else if(j==3)
				println("node "+nodeID+" receive the "+j+"rd message, hop is "+hops)
			else
				println("node "+nodeID+" receive the "+j+"th message, hop is "+hops)
			if(ArrivalCount==numRequests)
			{
				master! End(indexID,totalhop)
			}
                     
		}
		
		case "stop"=>
			System.exit(1)
		
		case _=>
		{
		}
	} 
  }

  val mymax=math.pow(2,bits).toLong
  
  class Master(numNodes: Int, numRequests:Int) extends Actor with ActorLogging 
  {
    var count = 0
    var totalhops=0
    var NetWork = HashMap.empty[String, ActorRef] 
    var mymap = new TreeMap[Int,String]
    override def preStart() 
	{
        for (i <- 0 until numNodes)
		{
            var IP=getnetworkIp(NetWork)
            var ID=md((math.random*mymax).toLong)
            mymap +={i->ID}
            var ss=i
            var node=system.actorOf(Props(new Node(ss,ID, numRequests, self)))
            NetWork +={ID->node}
        }
        for(allnodes<-NetWork.keysIterator)
		{
			NetWork(allnodes)! Start(NetWork,mymap)
		}   
    }
	
	def receive = {
		
		case End(nodeindex,nodehop) =>
		{
            totalhops+=nodehop
            count+=1
            if (count==numNodes)
			{
				println("the whole network's average hops is :"+(totalhops/(numRequests.toDouble*numNodes.toDouble)))
                for(allnode<-NetWork.keysIterator)
				{
					NetWork(allnode)!"stop"
				}
                System.exit(1)
            }
        }
       
		case _ =>
		{
		}
	}
  }
}
