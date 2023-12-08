import json
import threading
import time

import requests
import re

import unicodedata
from lxml import etree


def save(list_of_dicts, file_path):
    if file_path.endswith('.txt'):
        with open(file_path, "a") as file:
            for dict_ in list_of_dicts:
                file.write(str(dict_) + "\n")
    elif file_path.endswith('.json'):
        with open(file_path, "w") as file:
            json.dump(list_of_dicts, file, indent=4)


def find_values(json_data, key):
    results = []

    def search(json_data, key):
        if isinstance(json_data, dict):
            for k, v in json_data.items():
                if k == key:
                    results.append(v)
                elif isinstance(v, (dict, list)):
                    search(v, key)
        elif isinstance(json_data, list):
            for item in json_data:
                search(item, key)

    search(json_data, key)
    return results


def remove_unicode(text):
    # 使用NFKD规范化字符串，将Unicode字符分解为基本字符和组合字符
    normalized_text = unicodedata.normalize('NFKD', text)

    # 使用正则表达式删除所有非ASCII字符
    ascii_text = normalized_text.encode('ASCII', 'ignore').decode('ASCII')

    return ascii_text


def remove_html_tags(text):
    clean = re.compile('<.*?>')
    clean_text = re.sub(clean, '', text)
    # clean_text = remove_unicode(clean_text)
    return clean_text


class GoodRead:
    def __init__(self, path):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.203",
        }
        # self.comment = []
        self.ids = []  # "id":"kca://work/amzn1.gr.work.v1.jOPtnFSd_NuttiABTNjoEw"
        self.save_path = path
        self.error_path = "./error.txt"

    def get_html_url(self, book_id):
        return "https://www.goodreads.com/book/show/" + str(book_id)

    def get_comment_url(self):
        return "https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql"

    def get_html(self, book_id):
        comment = []
        # self.ids = []  # "id":"kca://work/amzn1.gr.work.v1.jOPtnFSd_NuttiABTNjoEw"
        list_of_dicts = []
        error_dicts = []
        basic_info = {
            "book_id": book_id,
            "book_name": "",
            "full_book_name": "",
            "score": 0,
            "author_name": "",
            "total_count": "",
            # "web_url": "",
            "genre": [],
            "author_description": "",
            "book_description": "",
            "comment": [],
        }

        # 访问
        url = self.get_html_url(book_id)
        response = requests.get(url=url, headers=self.headers).text
        tree = etree.HTML(response)
        json_content = tree.xpath("/html/body/script[1]")[0].text
        json_ = json.loads(json_content)
        # basic_info["web_url"] = url

        description = find_values(json_, "description")
        try:
            author_description = remove_html_tags(description[0])
            book_description = remove_html_tags(description[1])
        except IndexError:
            author_description = ""
            book_description = ""

        basic_info["author_description"] = author_description
        basic_info["book_description"] = book_description

        # token
        try:
            next_token = find_values(json_, "nextPageToken")[0]
            prev_token = find_values(json_, "prevPageToken")[0]
        except IndexError:
            next_token = ""
            prev_token = ""
        # print(json_)
        try:
            book_name = remove_html_tags(find_values(json_, "title")[0])
            author_name = remove_html_tags(find_values(json_, "name")[0])
            full_book_name = remove_html_tags(find_values(json_, "titleComplete")[0])
            average_rating = find_values(json_, "averageRating")[0]
            total_count = json_['props']['pageProps']['apolloState']['ROOT_QUERY']['getReviews']['totalCount']
        except KeyError:
            total_count = 0
        except IndexError:
            error_dicts.append(f"{book_id} not found error")
            save(error_dicts, self.error_path)
            return -1

        basic_info["author_name"] = author_name
        basic_info["book_name"] = book_name
        basic_info["full_book_name"] = full_book_name
        basic_info["total_count"] = total_count
        basic_info["score"] = average_rating

        # genre 书籍标签，例如惊悚，喜剧等
        ex_genre = r'{"__typename":"BookGenre","genre":{"__typename":"Genre","name":"(?P<genre>.*?)","webUrl":"(?P<genre_web_url>.*?)"}}'
        ex_genre_compile = re.compile(ex_genre, re.S)
        genre_iter = ex_genre_compile.finditer(json_content)
        for it in genre_iter:
            basic_info["genre"].append(it.group("genre"))
            # genre_web_url = it.group("genre_web_url")

        # id
        try:
            all_id = find_values(json_, "id")
            for i in all_id:
                if isinstance(i,str):
                    i.startswith('kca://work')
                    self.ids.append(i)
        except IndexError:
            error_dicts.append(f"{book_id} review is zero")
            save(error_dicts, self.error_path)
            return 0

        # comment
        next_token = self.get_comment("",comment)
        basic_info["comment"] = comment

        list_of_dicts.append(basic_info)
        i = 0
        while next_token != "":
            try:
                next_token = self.get_comment(next_token,comment)
            except Exception as e:
                error_dicts.append(f"{book_id} occurred {e}")
                save(error_dicts, self.error_path)
            print(f"\r{book_id},total--{total_count},当前进度：{i*100}")
            basic_info["comment"] = comment
            if i%15 == 0:
                list_of_dicts[-1]["comment"] = comment
                save(list_of_dicts, self.save_path)
            i += 1
        if total_count!=0 and len(comment)==0:
            raise Exception(f"networker error,{book_id}")
        save(list_of_dicts, self.save_path)
        return f"\r{book_id} finished"

    def get_comment(self, this_pagetoken, reviews):
        # 主评论
        response = ''
        for id in self.ids:
            max_retries = 3
            retry = 0
            while retry < max_retries:
                try:
                    response = self.req_graphql(this_pagetoken, id)
                    break
                except Exception:
                    retry += 1
                    if retry == max_retries:
                        raise
                    else:
                        print(f"Retrying... (Attempt {retry})")
                        time.sleep(0.5)
            if type(response) == str:
                continue
            if len(find_values(json.loads(response.text), "edges")) > 0 and len(find_values(json.loads(response.text), "edges")[0]) != 0:
                break
        if 'data' in response.json():
            comment_list = response.json()['data']['getReviews']['edges']
        else:
            return ""
        next_token_info = response.json()['data']['getReviews']['pageInfo']['nextPageToken']

        for i in range(len(comment_list)):
            if comment_list[i] is not None:
                comment = comment_list[i]['node']
            else:
                continue
            comment_text = comment['text']
            comment_text = remove_html_tags(comment_text)
            # commentcount = comment['commentCount']

            reviews.append(comment_text)

        if len(next_token_info) == 0:
            return ""

        return next_token_info

    def req_graphql(self, token, id, is_sub=False):
        url = self.get_comment_url()
        payload_dict = {
            "operationName": "getReviews",
            "variables": {
                "filters": {
                    "resourceType": "WORK",
                    "resourceId": f"{id}"
                },
                "pagination": {
                    "after": f"{token}",
                    "limit": 100
                }
            },
            "query": "query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {\n  getReviews(filters: $filters, pagination: $pagination) {\n    ...BookReviewsFragment\n    __typename\n  }\n}\n\nfragment BookReviewsFragment on BookReviewsConnection {\n  totalCount\n  edges {\n    node {\n      ...ReviewCardFragment\n      __typename\n    }\n    __typename\n  }\n  pageInfo {\n    prevPageToken\n    nextPageToken\n    __typename\n  }\n  __typename\n}\n\nfragment ReviewCardFragment on Review {\n  __typename\n  id\n  creator {\n    ...ReviewerProfileFragment\n    __typename\n  }\n  recommendFor\n  updatedAt\n  createdAt\n  spoilerStatus\n  lastRevisionAt\n  text\n  rating\n  shelving {\n    shelf {\n      name\n      webUrl\n      __typename\n    }\n    taggings {\n      tag {\n        name\n        webUrl\n        __typename\n      }\n      __typename\n    }\n    webUrl\n    __typename\n  }\n  likeCount\n  viewerHasLiked\n  commentCount\n}\n\nfragment ReviewerProfileFragment on User {\n  id: legacyId\n  imageUrlSquare\n  isAuthor\n  ...SocialUserFragment\n  textReviewsCount\n  viewerRelationshipStatus {\n    isBlockedByViewer\n    __typename\n  }\n  name\n  webUrl\n  contributor {\n    id\n    works {\n      totalCount\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment SocialUserFragment on User {\n  viewerRelationshipStatus {\n    isFollowing\n    isFriend\n    __typename\n  }\n  followersCount\n  __typename\n}\n"}

        if is_sub:
            payload_dict = {
                "operationName": "getComments",
                "variables": {
                    "filters": {
                        "resourceId": "kca://review:goodreads/amzn1.gr.review:goodreads.v1.HyAyJtlq-VtqskR2_VgfBw"
                    },
                    "perPage": 5
                },
                "query": "query getComments($filters: CommentFiltersInput!, $nextPageToken: String, $perPage: Int) {\n  getComments(\n    filters: $filters\n    pagination: {after: $nextPageToken, limit: $perPage}\n  ) {\n    edges {\n      node {\n        ...CommentFragment\n        __typename\n      }\n      __typename\n    }\n    totalCount\n    pageInfo {\n      nextPageToken\n      __typename\n    }\n    viewerCanComment\n    __typename\n  }\n}\n\nfragment CommentFragment on Comment {\n  __typename\n  id\n  text\n  updatedAt\n  createdAt\n  creator {\n    id: legacyId\n    imageUrlSquare\n    viewerRelationshipStatus {\n      isBlockedByViewer\n      __typename\n    }\n    isAuthor\n    name\n    webUrl\n    __typename\n  }\n}\n"
            }

        headers_graphql = {
            "Origin": "https://www.goodreads.com",
            "Accept-Encoding": "gzip,deflate,br",
            "Accept-Language": "zh - CN, zh;q = 0.9, en;q = 0.8, en - GB;q = 0.7, en - US;q = 0.6",
            "User-Agent": self.headers["User-Agent"],
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Referer": "https://www.goodreads.com/",
            "X-Api-Key": "da2-oqbnu4fbwrbtlf5xiuslyifrti",
        }
        response = ''
        # try:
        #     import http.client
        #     url = 'kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com'
        #     conn = http.client.HTTPSConnection(url)
        #     # 构建请求
        #     payload = json.dumps({"query": f"{payload_dict}"})
        #     conn.request("POST", "/graphql", body=payload, headers=headers_graphql)
        #
        #     # 获取响应
        #     response = conn.getresponse()
        #     print(response.read())
        # except requests.exceptions.RequestException as e:
        #     print(f"发生请求异常: {e}")
        # if response == '':
        try:
            response = requests.post(url=url, headers=headers_graphql, json=payload_dict)
        except Exception:
            raise
        return response


def process_book_ids(start_id, num_ids):
    for book_id in range(start_id, start_id + num_ids):
        save_path = f"./data/book{book_id}.json"
        goodread = GoodRead(save_path)
        status = goodread.get_html(book_id)
        print(status)


if __name__ == '__main__':
    import os

    if not os.path.exists('./data'):
        os.mkdir('./data')

    start_id = 10210 # 起始 book_id
    end_id = 10211
    # num_ids = 2  # 要处理的 book_id 数量
    num_ids = end_id-start_id+1

    num_threads = 2  # 启动的线程数量

    # 计算每个线程需要处理的 book_id 数量
    ids_per_threads = num_ids // num_threads

    # 创建多个线程
    threads = []
    for i in range(num_threads):
        start = start_id + i * ids_per_threads
        if i == num_threads - 1:
            end = start_id + num_ids  # 最后一个进程处理剩余的 book_id
        else:
            end = start + ids_per_threads
        thread = threading.Thread(target=process_book_ids, args=(start, end - start))
        threads.append(thread)

    # 启动并等待进程完成
    for process in threads:
        process.start()

    for process in threads:
        process.join()

    print("所有进程已完成")
