from selenium import webdriver
from selenium.webdriver.support import expected_conditions as ec
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

class Scrapper:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # self.driver = webdriver.Remote("http://172.26.0.2:4444/wd/hub", DesiredCapabilities.CHROME.copy())
        self.timeout = 30

    def get_elements_by_class(self, class_name, mark_element: WebElement = None):
        elements = None
        if mark_element:
            elements = mark_element.find_elements(By.CLASS_NAME, class_name)
        else:
            elements = self.driver.find_elements(By.CLASS_NAME, class_name)
        return elements

    def get_element_by_class(self, class_name, mark_element: WebElement = None):
        element = None
        if mark_element:
            element = mark_element.find_element(By.CLASS_NAME, class_name)
        else:
            element = self.driver.find_element(By.CLASS_NAME, class_name)
        return element

    def get_elements_by_tag(self, tag_name, mark_element: WebElement = None):
        elements = None
        if mark_element:
            elements = mark_element.find_elements(By.TAG_NAME, tag_name)
        else:
            elements = self.driver.find_elements(By.TAG_NAME, tag_name)
        return elements

    def get_element_by_tag(self, tag_name, mark_element: WebElement = None):
        element = None
        if mark_element:
            element = mark_element.find_element(By.TAG_NAME, tag_name)
        else:
            element = self.driver.find_element(By.TAG_NAME, tag_name)
        return element

    def wait_for_presence_of_element(self):
        pass

    def wait_for_presence_all_elements(self, class_name):
        WebDriverWait(self.driver, self.timeout) \
            .until(ec.presence_of_all_elements_located((By.CLASS_NAME, class_name)))

    def scroll_to_element_action(self, element: WebElement):
        ActionChains(self.driver) \
            .scroll_to_element(element) \
            .perform()
        
    def wait_and_perform_click_element(self, class_name):
        try:
            element = WebDriverWait(self.driver, self.timeout) \
                .until(ec.element_to_be_clickable((By.CLASS_NAME, class_name)))
            element.click()
        except ElementClickInterceptedException as e:
            self.driver.execute_script("arguments[0].click();", element)

    def execute(self, element: WebElement):
        self.driver.execute_script("arguments[0].click();", element)

    def start_setup(self, url):
        self.driver.get(url)
    
    def teardown(self):
        self.driver.quit()