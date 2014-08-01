package com.bigdata.mapreduce.seqtotext;
/*********************************************************************************************************
 **           Mapper 
 **           formatProject/FormatConverterTextToSequence/src/FormatConverterMapper.java 
 **           Reads text file and emits the contents out as key-value pairs
 *********************************************************************************************************/

import java.io.IOException;
import java.io.StringReader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
public class SequenceToTextMapper extends
Mapper<Text, Text, Text, Text> {
	Logger logger = Logger.getLogger(SequenceToTextMapper.class);
	
	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
	/*	byte[] bytes = value.getBytes();
		String xmlString = new String(bytes);*/
		String xmlString = value.toString();
		String rptTagValue = "";
		String outputStringPerMapper = "";
		try {
			Document doc = loadXMLFromString(xmlString.trim());
			doc.getDocumentElement().normalize();
			System.out.println("Root element :" + doc.getDocumentElement().getNodeName());
			
			NodeList indexList = doc.getElementsByTagName("index");
			Node indexList1 = indexList.item(0);

			String outString = "";	
			boolean previousSet = false;
			String startTime = "";
			String endTime = "";
			String callId = "";
			String appRegion = "";
			String previousRptTag = "0";
			String aniNumberTag0000 = "";
			String aniNumberTag1410 = "";	
			String aniNumberTag1420 = "";
			String aniNumberTag1440 = "";
			String dnis = "";
			String updatedAO = "";
			String dnisType = "";
			String accountKey = "";
			String dslColor = "";
			String uverseColor = "";
			String telcoAutoPayCurrent = "";
			String destinationAgent = "";
			boolean valid1410TN = false;
			boolean valid1420TN = false;
			boolean valid1440TN = false;
			boolean valid1027BAN = false;
			boolean valid1024BAN = false;	
			String ban1024 = "";
			String validTN = "";
			String initialAO = "";
			String aoType = "";
			boolean hungup = false;
			//String tnCollected = "";
			String tn = "";
			String tnAnswer1420 = "";
			String tnAnswer1440 = "";
			String ban = "";
			String banCollected ="";
			String tnCollected1410 ="";
			String tnCollected1420 ="";
			String tnCollected1440 ="";
			String ban1024status = "";
			String ban1024Confirmed = "";
			boolean agentTransferRule = false;
			boolean callReleased = false;
			boolean agentTransferRequested = false;
			boolean agentTransferForced = false;
			boolean agentTransferDefault = false;
			boolean agentTransferRequestedPOC = false;
			String infoDestinationName = "";
			String allRptTags = "";
			long elapsedTime = 0 ;
			boolean elapsedTimeSet = false;
			if(indexList1.getNodeType() == Node.ELEMENT_NODE) {

                                    Element eElement = (Element) indexList1;

				//StartTime
                                    startTime = getTagValue("startTime",eElement);
				//Region
                                   	appRegion = getTagValue("appRegion",eElement);
				//EndTime	
                                   	endTime =  getTagValue("endTime",eElement);
				callId = getTagValue("callID",eElement);
                    	}


			NodeList receipientList = doc.getElementsByTagName("rptTag");
	
			for (int i=0; i<receipientList.getLength(); i++) {

                            	Element nNode = (Element) receipientList.item(i);

                           	 	if(nNode.getNodeType() == Node.ELEMENT_NODE) {

                                    	Element eElement = (Element) nNode;
					rptTagValue = getTagValue("name", eElement);
					elapsedTimeSet = true;

				}	
				NodeList attribList = nNode.getElementsByTagName("attrib");

                                    for (int j=0; j<attribList.getLength(); j++) {

                                            Element attribNode = (Element) attribList.item(j);

					if(attribNode.getNodeType() == Node.ELEMENT_NODE) {

                                                    Element aElement = (Element) attribNode;
                                                    String attribName = getTagValue("name",aElement);

						if (rptTagValue.equals("0000")) {

							if(attribName.equals("Info_ANI_Number")) {

								aniNumberTag0000 = getTagValue("value", aElement);
							}
							if(aniNumberTag0000.equals("NULL_VALUE")) {

								aniNumberTag0000 ="";
							}
							if (attribName.equals("Info_DNIS")) {
							
								dnis = getTagValue("value", aElement);
							}
							if(dnis.equals("NULL_VALUE")) {

								dnis = "";
							}
						}

						
					 	if(previousSet) {


							if(attribName.equals("TC_ElapsedTime")) {

                                                           		elapsedTime = Long.parseLong(getTagValue("value", aElement));
								
								if((elapsedTime > 500) && elapsedTimeSet) {

                                                            		if(allRptTags.length() > 0) {

                                                                   			allRptTags = allRptTags + rptTagValue + "=" + String.valueOf(elapsedTime) + ":";
                                                            		}
                                                            		else {

                                                                   			allRptTags = rptTagValue + "=" + elapsedTime + ":";
                                                            		}
									elapsedTimeSet = false;
								}		
							}

							if(rptTagValue.equals("1410")) {

								if(attribName.equals("Info_ANI_Number")) {

                                                                       	aniNumberTag1410 = getTagValue("value", aElement);
                                                                    }
                                                                    if(attribName.equals("Info_ANI_Answer")) {

                                                                    	String status = getTagValue("value", aElement);
                                                                            if (status.equals("Y")) {

                                                                            	valid1410TN = true;
                                                                                   	validTN = aniNumberTag1410;
                                                                             }                                                 
                                                                     }
							}												
							if(rptTagValue.equals("1440")) {

								if(attribName.equals("Info_TN_Collected_Ind")) {

                                                                   		tnCollected1440 = getTagValue("value", aElement);
                                                                    }
                                                                    if((attribName.equals("Info_TN")) && (tnCollected1440.equals("1"))) {

                                                                    	aniNumberTag1440 = getTagValue("value", aElement);
                                                                  	} 
                                                                    if((attribName.equals("Info_TN_Answer")) && (tnCollected1440.equals("1"))) {

                                                                    	tnAnswer1440 = getTagValue("value", aElement);
                                                                    }
                                                                    if((tnCollected1440.equals("1")) && (aniNumberTag1440.length() == 10) && (tnAnswer1440.equals("TN"))) {

                                                                       	valid1440TN = true;
                                                                            validTN = aniNumberTag1440;
								}
                                                         	}        
							if(rptTagValue.equals("1027")) {

								if(attribName.equals("Info_BAN_Collected_Ind")) {

									banCollected = getTagValue("value", aElement);
                                                                    }
                                                                   	if((attribName.equals("Info_BAN")) && (banCollected.equals("1"))) {
							
									valid1027BAN = true;
									ban = getTagValue("value", aElement);
								}
							}
							if(rptTagValue.equals("1023")) {

                                                               	if(!valid1027BAN) {

                                                                   		if(attribName.equals("Info_BAN_Collected_Ind")) {

                                                                           		banCollected = getTagValue("value", aElement);
                                                                            }
                                                                            if((attribName.equals("Info_BAN")) && (banCollected.equals("1"))) {
                                                                            		
										valid1027BAN = true;
                                                                                   	ban = getTagValue("value", aElement);
                                                                            }
                                                                    }
                                                           	}
							if (rptTagValue.equals("1024")) { 

								if(!valid1027BAN) {

									if(attribName.equals("DM_Selection")) {

										ban1024 = getTagValue("value", aElement);
									}
                                                                           	if(attribName.equals("Info_BAN_Collected_Ind")) {

                                                                           		banCollected = getTagValue("value", aElement);

										if (banCollected.equals("1")) {

											valid1024BAN = true;
										}
									}
                                                                     }
							}
							if(valid1024BAN && (rptTagValue.equals("TRANS_Collect_TSP_BAN")))  {

								if(attribName.equals("Result")) {

									ban1024status = getTagValue("value", aElement);
								}
								if(attribName.equals("Reason")) {

									ban1024Confirmed = getTagValue("value", aElement); 
								}

								if(ban1024status.equals("SUCC") && ban1024Confirmed.equals("BAN_Confirmed")) {

									ban = ban1024;
								}
							}
								
							if (rptTagValue.equals("1420")) {	

								if(attribName.equals("Info_TN_Collected_Ind")) {

									tnCollected1420 = getTagValue("value", aElement);
								}
								if((attribName.equals("Info_TN")) && (tnCollected1420.equals("1"))) {

									aniNumberTag1420 = getTagValue("value", aElement);
								}
								if((attribName.equals("Info_TN_Answer")) && (tnCollected1420.equals("1"))) {

									tnAnswer1420 = getTagValue("value", aElement);
									
									if((tnCollected1420.equals("1")) && (aniNumberTag1420.length() == 10) && (tnAnswer1420.equals("TN"))) {
										valid1420TN = true;
										validTN = aniNumberTag1420;
									}
								}

							}
							if(rptTagValue.equals("1102")) {

								if(attribName.equals("INFO_INITIAL_AO")) {
								
									initialAO = getTagValue("value", aElement);
								}
								if(attribName.equals("INFO_AO_TYPE")) {

									aoType = getTagValue("value", aElement);
								}
							}		
							if(rptTagValue.equals("1003")) {

								if(attribName.equals("Info_Updated_AO")) {
										
									updatedAO = getTagValue("value", aElement);
								}
								if(attribName.equals("Info_DNIS_Type")) {

									dnisType = getTagValue("value", aElement);
								}
							}	
							if(rptTagValue.equals("1030")) {
								
								if(attribName.equals("INFO_N_ACCOUNT_KEY")) {

									accountKey = getTagValue("value", aElement);
								}
								if(accountKey.equals("NULL_VALUE")) {
							
									accountKey = "";
								}
							}		

							if(rptTagValue.equals("9954")) {

                                                                  	if(attribName.equals("Info_Destination_Address")) {

									destinationAgent = getTagValue("value", aElement);
								}
							}
							if(rptTagValue.equals("9957")) {

                                                                 	if(attribName.equals("Info_Destination_Address")) {

                                                                   		destinationAgent = getTagValue("value", aElement);
                                                                    }
                                                             }
							if(attribName.equals("Info_Agent_Transfer_Reason")) {

                                                            	String rule = getTagValue("value", aElement);
                                                                   	if(rule.equals("REQUESTED")) {

                                                                   		agentTransferRequested = true;
                                                                   	}
                                                            }
							if(attribName.equals("Info_Agent_Transfer_Reason")) {

                                                            	String rule = getTagValue("value", aElement);
                                                                   	if(rule.equals("ROUTING_RULE")) {

                                                                   		agentTransferRule = true;
								}
                                                            }
							if(attribName.equals("Info_Agent_Transfer_Reason")) {

                                                            	String rule = getTagValue("value", aElement);
                                                                   	if(rule.equals("FORCED")) {

                                                                   		agentTransferForced = true;
                                                                   	}
                                                            }

							if(attribName.equals("Info_Agent_Transfer_Reason")) {

                                                            	String rule = getTagValue("value", aElement);
                                                                   	if(rule.equals("DEFAULT")) {

                                                                   		agentTransferDefault = true;
                                                                   	}
                                                            }
							if(attribName.equals("Info_Agent_Transfer_Reason")) {

                                                            	String rule = getTagValue("value", aElement);
                                                                   	if(rule.equals("REQUESTED_POC")) {

                                                                   		agentTransferRequestedPOC = true;
                                                                   	}
							}

							if(attribName.equals("Info_Destination_Name")) {

								infoDestinationName = getTagValue("value", aElement);
							}

                                                            if(attribName.equals("TC_TransferTo")) {

                                                                   	previousRptTag = getTagValue("value", aElement);
								
								if(previousRptTag.equals("Hang-up")) {

                                                                           	hungup = true;
                                                                   	}
                                                            }
							
                                                    }	
						if(!previousSet) {

                                                            if(attribName.equals("TC_TransferTo")) {

                                                                    previousRptTag = getTagValue("value", aElement);
                                                                    previousSet = true;
                                                            }
                                                    }
					}
				}
			}
			outString = startTime + "|" + endTime + "|" + callId + "|" +  dnis + "|" + dnisType + "|" +  aniNumberTag0000 + "|" + validTN +  "|" + ban + "|" + accountKey  + "|" + updatedAO + "|" + initialAO + "|" + aoType + "|";
		    logger.info("************"+outString);
			boolean agentSet = false;
	
			if(agentTransferRule && !hungup) {

				outString = outString + "AGENTTRANSFERRULE" +  "|";
				agentSet = true;
			}
			if(agentTransferRequested && !hungup) {

				outString = outString + "AGENTTRANSFERREQUESTED" +  "|";	
				agentSet = true;
			}
			if (agentTransferForced && !hungup) {

				outString = outString + "AGENTTRANSFERFORCED" +  "|";
				agentSet = true;
			}
			if (agentTransferDefault && !hungup) {

                                    outString = outString + "AGENTTRANSFERDEFAULT" +  "|";
				agentSet = true;
                            }
			if (agentTransferRequestedPOC && !hungup) {

                                    outString = outString + "AGENTTRANSFERREQUESTEDPOC" +  "|";
				agentSet = true;
                            }

/*
			if((outString.matches("(.*)SEP(.*)")) && !agentSet && !hungup) {

				outString = outString + "SEP" +  "|";
			}
*/
			if(hungup) {

                                	outString = outString + "HANG-UP" +  "|";
                            }
			if(!hungup && !agentSet)  {

				outString = outString + "|";
			}

			logger.info("another logger *****"+outString);
			outputStringPerMapper = outputStringPerMapper + (outString + infoDestinationName + "|" + destinationAgent + "|" + allRptTags + "|EACREOR\n");
			logger.info("another logger 2 *****"+outputStringPerMapper);
			
			//
			context.write(key, new Text(outputStringPerMapper));
		} catch (Exception e) {
			e.printStackTrace();
		}
//		Text text = new Text(valueInBytes);
//		context.write(key, new Text(rptTagsId));
		//working
//		context.write(new Text(""), new Text(outputStringPerMapper));
	}

	public static Document loadXMLFromString(String xml) throws Exception
	{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		InputSource is = new InputSource(new StringReader(xml));
		return builder.parse(is);
	}

	private static String getTagValue(String sTag, Element eElement) {

                NodeList nlList = eElement.getElementsByTagName(sTag).item(0).getChildNodes();
                Node nValue = (Node) nlList.item(0);

		String returnValue = "";
                if(nlList.getLength() != 0 ) {

                        returnValue = nValue.getNodeValue();
                }
                //return nValue.getNodeValue();
                return returnValue;
        }
}

