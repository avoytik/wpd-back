import re
import mwparserfromhell
import datetime

from exceptions import *

class WikipediaPageParser:
  def __init__(self):
    self.categoryPeopleFromRegex = re.compile(r"(\[\[Category:People from .*\]\])", re.MULTILINE)
    self.attributesRegex = re.compile(r"\|[\s\t]*(?P<key>birth_date|death_date|birth_place)[\s\t]*=(?P<value>.*)$", re.MULTILINE)
    self.placeholdersRegex = re.compile(r"\[\[([^]|]+)\]\]", re.MULTILINE)
    self.placeholdersWithAlternateRegex = re.compile(r"\[\[([^]|]*)\|([^]|]*)?\]\]", re.MULTILINE)
    self.referencesRegex = re.compile(r"<ref[^>]*>([^<]+<\/ref>)?", re.MULTILINE)
    self.resourceRegex = re.compile(r"\[\[(Category|File|Image)([^][]|\[\[[^][]+\]\])+\]\]", re.MULTILINE)
    self.commentsRegex = re.compile(r"<!--[^>]*-->", re.MULTILINE)
    self.titleRegex = re.compile(r"'''([^']+)'''", re.MULTILINE)
    self.paragraphRegex = re.compile(r"([^\n]+)(?=\n)")
    self.dateRegex = re.compile(r"{[^|]+\|(?P<year>\d{4})\|(?P<month>\d{1,2})\|(?P<day>\d{1,2})[^}]*}")

  def needToParse(self, pageText):
    isPeopleCategory = self.categoryPeopleFromRegex.search(pageText)
    return isPeopleCategory is not None

  def parse(self, pageId, pageTitle, pageText):
    formattedText = self._cleanText(pageText)
    paragraphs = self.paragraphRegex.search(formattedText)

    pageData = {
      "page_id": pageId,
      "text": formattedText,
      "first_paragraph": paragraphs.group(0) if paragraphs is not None else "",
      "name": pageTitle
    }

    for attribute in self._getAttributes(pageText):
      pageData.update(attribute)

    return pageData

  def _cleanText(self, text):
    wikicode = mwparserfromhell.parse(self._cleanPageText(text).strip())

    return wikicode.strip_code()

  def _formatPlaceholders(self, text):
    return self._cleanText(self.placeholdersWithAlternateRegex.sub(
      r"\1 (\2)",
      self.placeholdersRegex.sub(
        r"\1",
        self.titleRegex.sub(r"\1", text)
      )
    ))

  def _cleanPageText(self, text):
    return self.commentsRegex.sub(
      "",
      self.resourceRegex.sub(
        "",
        self.referencesRegex.sub(
          "",
          text
        )
      )
    )

  def _formatDate(self, dateString):
    result = self.dateRegex.search(dateString)

    if result is None:
      raise InvalidDateException

    try:
      stringDate = f"{result.group('year')}-{result.group('month').zfill(2)}-{result.group('day').zfill(2)}"

      return datetime.datetime.strptime(stringDate, '%Y-%m-%d')
    except ValueError:
      raise InvalidDateException


  def _getAttributes(self, text):
    for attr in self.attributesRegex.finditer(text):
      match attr.group('key'):
        case "birth_date" | "death_date":
          yield { attr.group('key'): self._formatDate(attr.group('value').strip()) }
        case _:
          yield { attr.group('key'): self._formatPlaceholders(attr.group('value').strip()) }
