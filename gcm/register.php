<?php

// response json
$json = array();

/**
 * Registering a user device
 * Store reg id in users table
 */
//if (isset($_POST["name"]) && isset($_POST["email"]) && isset($_POST["regId"])) {
if(isset($_POST["imei"]) && isset($_POST["meid"])&& isset($_POST["simserial"])&& isset($_POST["androidid"]) && isset($_POST["regId"])) {
//    $name = $_POST["name"];
//    $email = $_POST["email"];

    $imei 	= $_POST["imei"];
    $meid 	= $_POST["meid"];
    $simserial 	= $_POST["simserial"];
    $androidid 	= $_POST["androidid"];            
    $gcm_regid 	= $_POST["regId"]; // GCM Registration ID
    
    // Store user details in db
    include_once './db_functions.php';
    include_once './GCM.php';

    $db = new DB_Functions();
    $gcm = new GCM();

    $res = $db->storeUser($imei, $meid, $simserial, $androidid, $gcm_regid);

    $registatoin_ids = array($gcm_regid);
    $message = array("product" => "shirt");

    $result = $gcm->send_notification($registatoin_ids, $message);

    echo $result;
} else {
    // user details missing
}
?>