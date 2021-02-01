import unittest

from get_sd_ou import classUtil

# classUtil.SearchPage.db_hash
# classUtil.SearchPage.get_articles
# classUtil.SearchPage.next_page()
# classUtil.SearchPage.curent_page_num
# classUtil.SearchPage.total_article_count

# classUtil.Article
# classUtil.Author

# classUtil.Journal
# classUtil.JournalsSearch

# classUtil.Page
# classUtil.Url

class TestSearchPage(unittest.TestCase):
    def test_get_articles(self):
        """
        Test that it can sum a list of integers
        """
        excpected_result = [
        'https://www.sciencedirect.com/science/article/pii/S0021925819761043',
        'https://www.sciencedirect.com/science/article/pii/S0021925819761055',
        'https://www.sciencedirect.com/science/article/pii/S0021925819761067',
        'https://www.sciencedirect.com/science/article/pii/S0021925819762802',
        'https://www.sciencedirect.com/science/article/pii/S0021925819762826',
        'https://www.sciencedirect.com/science/article/pii/S0895717710005777']

        url = "https://www.sciencedirect.com/search?date=2010&show=100"
        search_page = classUtil.SearchPage(url=url)
        result = sorted(search_page.get_articles())
        
        self.assertEqual(result[0:3] + result[-3:], excpected_result)
    
    def next_page(self):
        self.assertTrue(True)
    
    def curent_page_num(self):
        self.assertTrue(True)
        return True

    def total_article_count(self):
        self.assertTrue(True)

class TestArticle(unittest.TestCase):
    def a():
        pass

class TestAuthor(unittest.TestCase):
    def a():
        pass


class TestJournalsSearch(unittest.TestCase):
    def a():
        pass

class TestJournal(unittest.TestCase):
    def a():
        pass

if __name__ == '__main__':
    unittest.main()
