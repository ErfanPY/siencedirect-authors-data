-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Server version:               10.4.13-MariaDB - mariadb.org binary distribution
-- Server OS:                    Win64
-- HeidiSQL Version:             11.0.0.5919
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;


-- Dumping database structure for sciencedirect
CREATE DATABASE IF NOT EXISTS `sciencedirect` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
USE `sciencedirect`;

-- Dumping structure for table sciencedirect.articles
CREATE TABLE IF NOT EXISTS `articles` (
  `article_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `pii` varchar(20) NOT NULL UNIQUE,
  `title` text DEFAULT NULL,
  PRIMARY KEY (`article_id`)
) ENGINE=InnoDB AUTO_INCREMENT=60 DEFAULT CHARSET=utf8mb4;

-- Data exporting was unselected.

-- Dumping structure for table sciencedirect.authors
CREATE TABLE IF NOT EXISTS `authors` (
  `author_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `email` varchar(50) DEFAULT NULL UNIQUE,
  `scopus` varchar(50) DEFAULT NULL UNIQUE,
  `affiliation` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`author_id`)
) ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4;

-- Data exporting was unselected.

-- Dumping structure for table sciencedirect.article_authors
CREATE TABLE IF NOT EXISTS `article_authors` (
  `article_id` int(10) unsigned NOT NULL,
  `author_id` int(10) unsigned NOT NULL,
  `is_corresponde` bit(1) DEFAULT NULL,
  PRIMARY KEY (`article_id`,`author_id`),
  KEY `article_id` (`article_id`),
  KEY `author_id` (`author_id`),
  CONSTRAINT `FK_article_authors_articles` FOREIGN KEY (`article_id`) REFERENCES `articles` (`article_id`),
  CONSTRAINT `FK_article_authors_authors` FOREIGN KEY (`author_id`) REFERENCES `authors` (`author_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data exporting was unselected.

-- Dumping structure for table sciencedirect.authors
CREATE TABLE IF NOT EXISTS `searchs` (
  `search_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `date` varchar(50) DEFAULT NULL,
  `qs` varchar(50) DEFAULT NULL,
  `pub` varchar(50) DEFAULT NULL,
  `authors` varchar(50) DEFAULT NULL,
  `affiliation` varchar(50) DEFAULT NULL,
  `volume` varchar(50) DEFAULT NULL,
  `issue` varchar(50) DEFAULT NULL,
  `page` varchar(50) DEFAULT NULL,
  `tak` varchar(50) DEFAULT NULL,
  `title` varchar(50) DEFAULT NULL,
  `refrences` varchar(50) DEFAULT NULL,
  `docId` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`search_id`)
) ENGINE=InnoDB AUTO_INCREMENT=112 DEFAULT CHARSET=utf8mb4;

-- Data exporting was unselected.

-- Dumping structure for table sciencedirect.article_authors
CREATE TABLE IF NOT EXISTS `search_articles` (
  `search_id` int(10) unsigned NOT NULL,
  `article_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`search_id`,`article_id`),
  KEY `search_id` (`search_id`),
  KEY `article_id` (`article_id`),
  CONSTRAINT `FK_search_articles_searchs` FOREIGN KEY (`search_id`) REFERENCES `searchs` (`search_id`),
  CONSTRAINT `FK_search_articles_articles` FOREIGN KEY (`article_id`) REFERENCES `articles` (`article_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Data exporting was unselected.



/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IF(@OLD_FOREIGN_KEY_CHECKS IS NULL, 1, @OLD_FOREIGN_KEY_CHECKS) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
