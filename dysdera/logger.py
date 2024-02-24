"""
This file contains only a single class for logs on the program state
"""
import logging
from dysdera.parser import URL


class DysderaLogger:
    """
    Class for managing logs through a log file and the terminal
    """

    class DysderaLogFormatter(logging.Formatter):

        def format(self, record):
            record.url = getattr(record, 'url', 'N/A')
            return super().format(record)

    def __init__(self, verbose: bool, verbose_log: bool):
        """
        params:
            verbose_log     if True also generical informations will be written (errors and warnings are always written in the log file)
            verbose         if True errors, warnings and generical informations are printed else nothing
        """
        self.verbose = verbose
        self.verbose_log = verbose_log
        self.logger = logging.getLogger('logger')
        if not self.logger.handlers:
            file_handler = logging.FileHandler('dysdera.log')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(
                self.DysderaLogFormatter('%(asctime)s - %(levelname)s - URL: %(url)s - Routine: %(routine)s - %('
                                         'message)s'))
            self.logger.addHandler(file_handler)
            self.logger.setLevel(logging.INFO)

    def err_output(self, err: str, routine: str, blame: URL | str = ''):
        """
        params:
            err             error info
            routine         the task in witch the error occured
            blame           url or "thing" that caused the error
        """
        val = blame() if isinstance(blame, URL) else blame
        if self.verbose:
            print("ERROR " + (val if val != '' else "") + " " + err + f" during {routine}")
        self.logger.error(err, extra={'url': f'{val}', 'routine': f'{routine}'})

    def warn_output(self, warn: str, routine: str, blame: URL | str = ''):
        """
        params:
            err             warning info
            routine         the task in witch the warning occured
            blame           url or "thing" that caused the warning
        """
        val = blame() if isinstance(blame, URL) else blame
        if self.verbose:
            print("WARING " + (val if val != '' else "") + " " + warn + f" during {routine}")
        self.logger.warning(warn, extra={'url': f'{val}', 'routine': f'{routine}'})

    def info_output(self, info: str, routine: str, at: URL | str = ''):
        """
        params:
            info            what happend?
            routine         the task in execution
            at              url or "thing" that you are manipulating now
        """
        val = at() if isinstance(at, URL) else at
        if self.verbose:
            print(info + ((" from page: " + val) if val != '' else "") + f" during {routine}")
        if self.verbose_log:
            self.logger.info(info, extra={'url': f'{val}', 'routine': f'{routine}'})
