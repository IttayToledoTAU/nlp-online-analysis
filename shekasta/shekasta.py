from lxml import html
import pandas as pd
from datetime import datetime
import re
import concurrent.futures
import requests


headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 \
    (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3'}


def page_content_print_time(thread_id):
    df_posts, df_users = page_content(thread_id)
    return df_posts, df_users


def thread_ids(pages, verbose=True):
    print('Fetching pages...')
    count = 0
    base_main_url = 'https://www.fxp.co.il/forumdisplay.php?f=46&page=%d'
    thread_ids = []
    for i in pages:
        main_url = base_main_url % i
        response = requests.get(main_url)
        contents = response.content.decode("utf-8")
        response.close()
        tree = html.fromstring(contents)
        thread_ids += [int(x[7:]) for x
                       in tree.xpath('//ol[@id=\'threads\']/li/@id')]
        count += 1
        if verbose and (count % 10 == 0):
            print('Fetched %s pages' % (count))
            print(datetime.now().time())
    # thread_ids.remove(12069815)
    return thread_ids


def page_content(thread_id):
    df_thread_new = pd.DataFrame(columns=['thread_id', 'title'])
    df_post_new = pd.DataFrame(columns=['thread_id', 'post_id', 'user_name', 'date', 'message',
                                        'cite1', 'cite2', 'cite3', 'cite4'])
    df_user_new = pd.DataFrame(
        columns=['user_name', 'register_date', 'message_count', 'signiture_text'])
    page = 1
    url = 'https://www.fxp.co.il/showthread.php?t=%s' % (thread_id)
    response_page = None
    last_page = 1
    while page <= last_page:
        if page > 1:
            url = 'https://www.fxp.co.il/showthread.php?t=%s&page=%s' % (
                thread_id, str(page))
        response_page = requests.get(url)
        tree = html.fromstring(response_page.content.decode("utf-8"))
        if page == 1:
            last_page = tree.xpath('//span[@class=\'first_last\']/a/@href')
            last_page = 1 if len(last_page) == 0 else int(
                re.search('page=([0-9]+)', last_page[0]).group(1))
            title = tree.xpath('//title/text()')[0]
            df_thread_new = df_thread_new.append(
                {'thread_id': thread_id, 'title': title}, ignore_index=True)
        df_post_new_, df_user_new_ = thread_single_page_content(
            thread_id, tree, page)
        df_post_new = pd.concat([df_post_new, df_post_new_])
        df_user_new = pd.concat([df_user_new, df_user_new_])
        page += 1
    return df_thread_new, df_post_new, df_user_new


def thread_single_page_content(thread_id, tree, page):
    df_post_new = pd.DataFrame(columns=['thread_id', 'post_id', 'user_name', 'date', 'message',
                                        'cite1', 'cite2', 'cite3', 'cite4'])
    df_user_new = pd.DataFrame(
        columns=['user_name', 'register_date', 'message_count', 'signiture_text'])

    posts = tree.xpath(
        '//li[@class=\'postbit postbitim postcontainer\']')
    for post in posts:
        post_id = post.attrib['id']
        message = ' '.join(post.xpath(
            '//blockquote[@class=\'postcontent restore\']/text()'))
        user_name = post.xpath(
            '//div[@class=\'username_container\']//a[starts-with(@class,\'username\')]/@title')[0][:-6]
        message_date = post.xpath('//span[@class=\'date\']/text()')[0]
        cites = post.xpath('//div[@class=\'bbcode_quote\']')
        cited_post = []
        for cite in cites:
            try:
                link = cite.xpath('//a')[0].attrib['href']
                cited_post.append(link[link.find('#') + 1:])
            except:
                cited_post.append('custom cites')
        user_details = post.xpath('//div[@class=\'userinfo\']/div[2]')[0]
        user_reg_date = user_details.xpath('div[1]/text()')[0]
        user_message_count = user_details.xpath('div[3]/text()')[0]
        try:
            signiture_text = ' '.join(post.xpath(
                '//blockquote[@class=\'signature restore\']//text()'))
        except:
            signiture_text = ''
        df_post_new = df_post_new.append({'thread_id': thread_id, 'post_id': post_id, 'user_name': user_name, 'date': message_date,
                                          'message': message, 'cite1': cited_post[0] if len(cites) > 0 else '',
                                          'cite2': cited_post[1] if len(cites) > 1 else '',
                                          'cite3': cited_post[2] if len(cites) > 2 else '',
                                          'cite4': cited_post[3] if len(cites) > 3 else ''}, ignore_index=True)
        df_user_new = df_user_new.append({'user_name': user_name, 'register_date': user_reg_date,
                                          'message_count': user_message_count, 'signiture_text': signiture_text}, ignore_index=True)
    return df_post_new, df_user_new


def thread_content(thread_ids, thread_file='thread.csv', post_file='post.csv', user_file='user.csv', verbose=True):
    print('Fetching threads...')
    count = 0
    df_thread_new = pd.DataFrame(columns=['thread_id', 'title', 'type'])
    df_post_new = pd.DataFrame(columns=['thread_id', 'post_id', 'user_name', 'date', 'message',
                                        'cite1', 'cite2', 'cite3', 'cite4'])
    df_user_new = pd.DataFrame(
        columns=['user_name', 'register_date', 'message_count', 'signiture_text'])
    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        future_to_url = {executor.submit(
            page_content, thread_id): thread_id for thread_id in thread_ids}
        print(f'finish to submit all jobs')
        df = pd.DataFrame(columns=['thread', 'post'])
        for future in concurrent.futures.as_completed(future_to_url):
            df_thread_new_, df_post_new_, df_user_new_ = future.result()
            df_thread_new = pd.concat([df_thread_new, df_thread_new_])
            df_post_new = pd.concat([df_post_new, df_post_new_])
            df_user_new = pd.concat([df_user_new, df_user_new_])
    df_thread_new.to_csv(thread_file, encoding='utf-8')
    df_post_new.to_csv(post_file, encoding='utf-8')
    df_user_new.to_csv(user_file, encoding='utf-8')


print(datetime.now().time())
thread_content(thread_ids(range(1, 2)))
print(datetime.now().time())
