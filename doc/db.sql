-- MySQL dump 10.13  Distrib 5.1.61, for redhat-linux-gnu (x86_64)
--
-- Host: localhost    Database: community
-- ------------------------------------------------------
-- Server version	5.1.61

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `clusterstate`
--

DROP TABLE IF EXISTS `clusterstate`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `clusterstate` (
  `uid` varchar(20) NOT NULL,
  `state` tinyint(4) DEFAULT NULL,
  `time` bigint(20) DEFAULT NULL,
  `workerid` int(11) DEFAULT '-1',
  `errmsg` varchar(200) DEFAULT '""',
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `grouptags`
--

DROP TABLE IF EXISTS `grouptags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `grouptags` (
  `uid` varchar(20) NOT NULL,
  `groupid` varchar(45) NOT NULL,
  `tags` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`uid`,`groupid`),
  KEY `uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `neighgroups`
--

DROP TABLE IF EXISTS `neighgroups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `neighgroups` (
  `uid` varchar(20) NOT NULL,
  `friid` varchar(20) NOT NULL,
  `groupid` varchar(20) NOT NULL,
  KEY `uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tagstats`
--

DROP TABLE IF EXISTS `tagstats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tagstats` (
  `tag` varchar(50) NOT NULL,
  `num` int(11) DEFAULT NULL,
  PRIMARY KEY (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tweet`
--

DROP TABLE IF EXISTS `tweet`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `tweet` (
  `mid` varchar(20) NOT NULL,
  `rtmid` varchar(20) DEFAULT '-1',
  `text` varchar(1000) DEFAULT NULL,
  `source` varchar(100) DEFAULT NULL,
  `uid` varchar(20) DEFAULT NULL,
  `reposts_count` int(11) DEFAULT NULL,
  `comments_count` int(11) DEFAULT NULL,
  `created_at` date DEFAULT NULL,
  PRIMARY KEY (`mid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `usermids`
--

DROP TABLE IF EXISTS `usermids`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usermids` (
  `uid` varchar(20) NOT NULL,
  `mids` text,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `userprofile`
--

DROP TABLE IF EXISTS `userprofile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `userprofile` (
  `uid` varchar(20) NOT NULL,
  `name` varchar(45) DEFAULT NULL,
  `province` varchar(45) DEFAULT '',
  `city` varchar(45) DEFAULT '',
  `location` varchar(45) DEFAULT '',
  `descr` varchar(45) DEFAULT '',
  `url` varchar(45) DEFAULT '',
  `pimg_url` varchar(45) DEFAULT '',
  `gender` char(4) DEFAULT '',
  `folCount` int(11) DEFAULT NULL,
  `friCount` int(11) DEFAULT NULL,
  `statusCount` int(11) DEFAULT NULL,
  `favorCount` int(11) DEFAULT NULL,
  `createat` bigint(20) DEFAULT NULL,
  `verified` tinyint(4) DEFAULT NULL,
  `vtype` int(11) DEFAULT NULL,
  `verified_reason` varchar(100) DEFAULT '',
  `lastcrawltime` bigint(20) DEFAULT '-1',
  `crawlstate` tinyint(4) DEFAULT '2',
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `usertags`
--

DROP TABLE IF EXISTS `usertags`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `usertags` (
  `uid` varchar(20) NOT NULL,
  `tags` text,
  PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-09-08 10:41:56
