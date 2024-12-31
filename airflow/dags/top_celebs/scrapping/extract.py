from dags.top_celebs.scrapping.scrapper import Scrapper


def extract_celebs(ti):
    IMDB_CELEBS_POP_URL = 'https://www.imdb.com/chart/starmeter/?ref_=hm_mpc_sm'
    CLASS_NAME_TOP_CELEBS = "ipc-metadata-list-summary-item"

    scrapper = Scrapper()
    scrapper.start_setup(IMDB_CELEBS_POP_URL)
    scrapper.wait_for_presence_all_elements(CLASS_NAME_TOP_CELEBS)
    celebs_items = scrapper.get_elements_by_class(CLASS_NAME_TOP_CELEBS)

    celebs = []

    for celeb in celebs_items:
        celeb_name = scrapper.get_element_by_class("ipc-title__text", celeb).text
        roles_items = scrapper.get_elements_by_class("ipc-inline-list__item", celeb)

        celeb_roles = []
        for role in roles_items:
            celeb_roles.append(role.text)
        
        celeb_rank = scrapper.get_element_by_class("meter-const-ranking", celeb) \
            .text \
            .split(" ")[0]

        celeb_id = scrapper.get_element_by_class("ipc-lockup-overlay", celeb) \
            .get_attribute("href").split("/")[-2]
        celebs.append([celeb_id, celeb_name, celeb_roles, int(celeb_rank)])

    print(celebs)
    ti.xcom_push(key="celebs", value=celebs)