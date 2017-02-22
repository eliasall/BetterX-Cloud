<?php

    //$regid = $_POST['regid'];
	
	$regid = $_REQUEST["regid"];
    
    include_once './db_functions.php';
    
    $db = new DB_Functions();
    if( $regid != "" )
       $res = $db->deleteUser($regid);
    echo $res;
?>