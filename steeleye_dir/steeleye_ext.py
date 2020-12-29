import asyncio
import os
import uuid
import csv

import aiohttp

import async_timeout

import requests, zipfile, io
import xml.etree.ElementTree as ET
import pandas as pd

filed_names = []


def csv_file_write(csv_file_name, data_list):
    fieldnames = [
        'FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'FinInstrmGnlAttrbts.NtnlCcy', 'Issr'
    ]
    # {'FinInstrmGnlAttrbts.Id': 'BE0000348574', 'FinInstrmGnlAttrbts.FullNm': 'BGB 1.700 22/06/50 #88',
    #  'FinInstrmGnlAttrbts.ShrtNm': 'Belgique/1.7 Bd 20500622 Sr Gtd', 'FinInstrmGnlAttrbts.ClssfctnTp': 'DBFNFR',
    #  'FinInstrmGnlAttrbts.NtnlCcy': 'EUR', 'FinInstrmGnlAttrbts.CmmdtyDerivInd': 'false'}

    with open(csv_file_name, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # print(data_list)
        for data_dict in data_list:
            writer.writerow({k: data_dict.get(k) for k in fieldnames})


def child_read_xml_file(dir_path, file_name):
    try:
        csv_file_name = f"{dir_path}/{str(uuid.uuid4())}.csv"

        tree = ET.parse(file_name)
        root = tree.getroot()
        # printing the root (parent) tag
        # of the xml document, along with
        # its memory location
        print(root, csv_file_name)
        childrens = root.findall(".")[0].getchildren()[1].getchildren()[0].getchildren()[0].getchildren()
        print("checking the sub tags wait .........")
        sub_sub_children_list = list()
        for index, sub_children in enumerate(childrens):
            if index >= 1:
                for index1, sub_sub_children in enumerate(sub_children.getchildren()):
                    sub_sub_children_dict = dict()
                    if index1 == 0:
                        for sub_sub_sub_children in sub_sub_children.findall('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FinInstrmGnlAttrbts'):
                            for index2, child in enumerate(sub_sub_sub_children):
                                if child.tag == '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ShrtNm':
                                    continue
                                else:
                                    sub_sub_children_dict[
                                        child.tag.replace('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}', 'FinInstrmGnlAttrbts.')
                                    ] = child.text
                    elif index == 1:
                        for sub_children in sub_sub_children[index].findall('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr'):
                            sub_sub_children_dict[
                                sub_children.tag.replace('{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}', '')] = sub_children.text
                    else:
                        continue
                    sub_sub_children_list.append(sub_sub_children_dict)

            else:
                continue
        # print("sub_sub_children_dict", sub_sub_children_dict)
        csv_file_write(csv_file_name=csv_file_name, data_list=sub_sub_children_list)
    except Exception:
        print("Run the project with virtual environment")


def file_parse(dir_path, file_lists):
    for file_name in file_lists:
        print("file-name", file_name)
        try:
            child_read_xml_file(dir_path, file_name)
            # child_read_xml_file(dir_path, file_lists[0])
        except Exception:
            print("Run the project with virtual environment")


def read_xml_file(file_name):
    tree = ET.parse(file_name)
    root = tree.getroot()

    # printing the root (parent) tag
    # of the xml document, along with
    # its memory location
    print(root)
    str_elements = [form.findall('./str') for form in root.findall("./result/doc")]
    output = [
        {el_tag[index - 2].text: el_tag[index].text} for el_tag in str_elements for index in range(1, len(el_tag), 6)
    ]
    print(output)
    zip_urls = [v for i in output for k, v in i.items()]
    print("urls", zip_urls)
    return zip_urls


def run(urls, is_child_url):
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(main(urls, is_child_url))[0]
    print((results))
    return results


def create_dir():
    try:
        directory = "steel_eye_file_dir"
        path = os.path.join(os.getcwd(), directory)
        os.mkdir(path)
    except FileExistsError:
        pass
    return path


def extract_zip_file(path, response_content):
    z = zipfile.ZipFile(io.BytesIO(response_content))
    z.extractall(path)


def remove_file_path(file_name):
    if os.path.exists(file_name):
        print('removed the file' + file_name)
        os.remove(file_name)


async def get_url(url, session, is_child_url, dir_path):

    async with async_timeout.timeout(120):
        if is_child_url:
            with requests.get(url) as response:
                extract_zip_file(path=dir_path, response_content=response.content)
                return dir_path
        else:
            file_name = f"{str(uuid.uuid4())}.xml"
            async with session.get(url, ssl=False) as response:
                with open(file_name, 'wb') as fd:
                    async for data in response.content.iter_chunked(1024):
                        fd.write(data)
                zip_urls = read_xml_file(file_name)
                print('Successfully downloaded ' + file_name)
                # remove  the file path
                remove_file_path(file_name)
                return zip_urls


async def main(urls, is_child_url):
    async with aiohttp.ClientSession() as session:
        dir_path = create_dir()
        tasks = [get_url(url, session, is_child_url, dir_path) for url in urls]

        return await asyncio.gather(*tasks)


def get_list_files(directory_path, file_list):
    for entry in os.scandir(directory_path):
        if entry.is_file():
            # check .xml files
            if entry.name.endswith('.xml'):
                file_list.append(entry.path)
        else:
            get_list_files(entry.path, file_list)
    return file_list


if __name__ == '__main__':

    down_urls = ['https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2020-01-08T00:00:00Z+TO+2020-01-08T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100' ]  # noqa
    zip_urls = run(down_urls, is_child_url=False)
    dir_path = run(zip_urls, is_child_url=True)
    if not dir_path:
        print("directory-path is not found")
        exit(1)
    file_lists = get_list_files(dir_path, file_list=list())
    print("file_lists", file_lists)
    file_parse(dir_path, file_lists)
