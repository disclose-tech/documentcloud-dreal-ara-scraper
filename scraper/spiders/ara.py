import re
import os
from urllib.parse import urlsplit
from zipfile import ZipFile
from pathlib import Path
from datetime import datetime, timedelta

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.http import Request

from documentcloud.constants import SUPPORTED_EXTENSIONS

from ..items import DocumentItem


class ARASpider(scrapy.Spider):

    name = "DREAL ARA Scraper"

    # allowed_domains = ["auvergne-rhone-alpes.developpement-durable.gouv.fr"]

    start_urls = [
        "https://www.auvergne-rhone-alpes.developpement-durable.gouv.fr/projets-r3463.html?lang=fr"
    ]

    upload_limit_attained = False

    start_time = datetime.now()

    def check_time_limit(self):
        """Closes the spider automatically if it reaches a duration of 5h45min"""
        """as GitHub's actions have a 6 hours limit."""

        if self.time_limit != 0:

            limit = self.time_limit * 60
            now = datetime.now()

            if timedelta.total_seconds(now - self.start_time) > limit:
                raise CloseSpider(
                    f"Closed due to time limit ({self.time_limit} minutes)"
                )

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def parse(self, response):
        """Parse the starting page"""

        # Selecting the first menu ("Par département")
        rubrique_depts = response.css("#contenu .rubrique_avec_sous-rubriques")[0]

        depts = rubrique_depts.css(".fr-collapse .lien-sous-rubrique")

        for d in depts:
            dept_name = d.css("::text").get()
            link = d.css("a").attrib["href"]

            yield response.follow(
                link,
                callback=self.parse_years_list,
                cb_kwargs=dict(department=dept_name),
            )

    def parse_years_list(self, response, department):
        """Parse the year selection page for a department."""

        self.check_time_limit()
        self.check_upload_limit()

        if self.target_year > 2016:

            year_sections = response.css("#contenu .liste-rubriques > div")

            for section in year_sections:

                if section.css(".rubrique_avec_sous-rubriques"):
                    section_title = section.css("p.fr-tile__title::text").get().strip()
                elif section.css(".item-liste-rubriques-seule"):
                    section_title = section.css(".fr-tile__link::text").get().strip()

                if section_title == str(self.target_year):

                    if section.css(".rubrique_avec_sous-rubriques"):

                        links = section.css(".fr-collapse a")

                        for l in links:
                            link_url = l.attrib["href"]
                            link_text = l.css("::text").get().strip()

                            yield response.follow(
                                link_url,
                                callback=self.parse_projects_list,
                                cb_kwargs=dict(
                                    department=department, subdiv=link_text, page=1
                                ),
                            )

                    elif section.css(".item-liste-rubriques-seule"):

                        link_url = section.css("a").attrib["href"]

                        yield response.follow(
                            link_url,
                            callback=self.parse_projects_list,
                            cb_kwargs=dict(department=department, page=1),
                        )

        else:
            raise CloseSpider("Target year is 2016 or before. Not supported for now.")

    def parse_projects_list(self, response, department, page, subdiv=""):
        """Parse projects list for a year & department."""

        self.check_time_limit()
        self.check_upload_limit()

        self.logger.info(
            f"Scraping {department}{' ' + subdiv if subdiv else ''}, page {page}"
        )

        projects_links = response.css("#contenu .liste-articles .fr-card__link")

        for link in projects_links:
            link_text = link.css("::text").get()
            link_url = link.attrib["href"]

            yield response.follow(
                link_url,
                callback=self.parse_project_page,
                cb_kwargs=dict(department=department, subdiv=subdiv),
            )

        # next page

        next_page_link = response.css(
            "#contenu .fr-pagination__list .fr-pagination__link--next[href]"
        )

        if next_page_link:

            next_page_url = next_page_link.attrib["href"]

            yield response.follow(
                next_page_url,
                callback=self.parse_projects_list,
                cb_kwargs=dict(department=department, page=page + 1, subdiv=subdiv),
            )

    def parse_project_page(self, response, department, subdiv):
        """Parse the page of a project."""

        self.check_time_limit()
        self.check_upload_limit()

        project = response.css("h1.titre-article::text").get()

        # Get files

        file_links = response.css("#contenu div.fr-download a.fr-download__link")

        for link in file_links:
            link_text = link.css("::text").get().strip()
            link_url = link.attrib["href"]

            absolute_url = response.urljoin(link_url)

            if (
                absolute_url not in self.event_data["documents"]
                and absolute_url not in self.event_data["zips"]
            ):

                doc_item = DocumentItem(
                    title=link_text,
                    source_page_url=response.request.url,
                    project=project,
                    year=str(self.target_year),
                    authority="Préfecture de région Auvergne-Rhône-Alpes",
                    category_local="Les décisions au cas par cas - Projets",
                    source_scraper=f"DREAL ARA Scraper {self.target_year}",
                    department_from_scraper=re.search(r"\((\d\d)\)", department).group(
                        1
                    ),
                )

                yield response.follow(
                    link_url,
                    method="HEAD",
                    callback=self.parse_document_headers,
                    cb_kwargs=dict(
                        doc_item=doc_item, department=department, subdiv=subdiv
                    ),
                )

    def parse_document_headers(self, response, department, subdiv, doc_item):

        self.check_time_limit()
        self.check_upload_limit()

        doc_item["source_file_url"] = response.request.url

        doc_item["publication_lastmodified"] = response.headers.get(
            "Last-Modified"
        ).decode("utf-8")

        # Detect zip files and process them separately
        if doc_item["source_file_url"].lower().endswith(".zip"):

            if doc_item["source_file_url"] not in self.event_data["zips"]:
                yield Request(
                    url=response.request.url,
                    callback=self.parse_zip_file,
                    cb_kwargs=dict(doc_item=doc_item, department=department),
                )

        else:
            if doc_item["source_file_url"] not in self.event_data["documents"]:
                doc_item["file_from_zip"] = False
                yield doc_item

    def parse_zip_file(self, response, doc_item, department):

        self.check_time_limit()
        self.check_upload_limit()

        # Get the modification date of the zip in the headers
        publication_lastmodified = response.headers.get("Last-Modified").decode("utf-8")

        # Get the filename from the requested URL
        urlpath = urlsplit(response.request.url).path
        filename = os.path.basename(urlpath)

        # Create the folder to hold zip files if it does not exist yet
        if not os.path.exists("./downloaded_zips"):
            os.makedirs("./downloaded_zips")

        # Save the zip file in the folder
        with open(f"./downloaded_zips/{filename}", "wb") as file:
            file.write(response.body)

        # Create a folder to hold the extracted files
        extracted_files_folder = f"./downloaded_zips/{filename[:-4]}"
        if not os.path.exists(extracted_files_folder):
            os.makedirs(extracted_files_folder)

        # Open Zip file and extract files
        with ZipFile(f"./downloaded_zips/{filename}", "r") as zip_file:
            zip_file.extractall(path=extracted_files_folder)

        # Delete zip file
        os.remove(f"./downloaded_zips/{filename}")

        # List files
        extracted_files_folder_path_obj = Path(extracted_files_folder)
        extracted_files_list = list(extracted_files_folder_path_obj.rglob("*"))

        # Make a list of seen files for event_data
        zip_seen_supported_files = []

        for f in extracted_files_list:
            if f.is_file():
                basename = os.path.basename(str(f))

                filename, file_ext = os.path.splitext(basename)

                if file_ext.lower() in SUPPORTED_EXTENSIONS:

                    relative_filepath = "/".join(str(f).split("/")[2:])

                    zip_seen_supported_files.append(relative_filepath)

        # Yield a document object for each one
        for f in extracted_files_list:
            if f.is_file():
                filepath = str(f)

                item_relative_filepath = os.path.join(*filepath.split(os.sep)[2:])

                event_data_path = (
                    doc_item["source_file_url"] + "/" + item_relative_filepath
                )

                if not event_data_path in self.event_data["documents"]:
                    yield DocumentItem(
                        project=doc_item["project"],
                        category_local=doc_item["category_local"],
                        authority=doc_item["authority"],
                        source_scraper=f"DREAL ARA Scraper {self.target_year}",
                        source_file_url=response.request.url,
                        source_filename=f.name,
                        source_page_url=doc_item["source_page_url"],
                        publication_lastmodified=publication_lastmodified,
                        local_file_path=str(f),
                        zip_seen_supported_files=zip_seen_supported_files,
                        file_from_zip=True,
                        year=str(self.target_year),
                        department_from_scraper=doc_item["department_from_scraper"],
                    )
