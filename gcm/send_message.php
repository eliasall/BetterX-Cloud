<?php

	$regId   	= $_REQUEST["data1"]; // Get device id.
    	$msg	 	= $_REQUEST["data2"]; // Get message to send.
    
	include_once './db_functions.php';
    	include_once './GCM.php';

    	$db = new DB_Functions();    
	$gcm = new GCM(); // Create GCM instance.
	
	
	if( $msg != "" )
	{
	       $sender_regId = 0;
	       $registatoin_ids = array($regId);
	       $message = array("price" => $msg);
	       $result = $gcm->send_notification($registatoin_ids, $message );
	       $jsonArray = json_decode($result);
		   
	       if(!empty($jsonArray->results))
	       {
				$results = $jsonArray->results[0];
				if(isset($results->message_id)  && ( $msg !="delete_device" ) )
        	    {
        	    	$res = $db->saveMessage($sender_regId, $regId, $results->message_id, $msg);
            	}
				else
				{
					$resNew = $db->saveMessageJSON($sender_regId, $regId, NULL, $msg, $result);
				}
			}
			else
			{
				$resFailed = $db->saveMessage($sender_regId, $regId, "FAILED", $msg);
			}
		
		echo $result ;
	    	
	}
?>