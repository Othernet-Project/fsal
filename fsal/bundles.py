import os
import logging

import zippie

from .utils import common_ancestor


def extract_zip_bundle(bundle_path, extract_path):
    success = False
    files = []
    try:
        zfile = zippie.PieZipFile(bundle_path)
        files = zfile.namelist()
        # TODO: Add check for testing integrity of zip bundle
        zfile.extractall(extract_path)
        success = True
    except (RuntimeError, zippie.BadZipFile) as e:
        logging.exception('Error while extracting zip bundle: {}'.format(str(e)))
    return success, files


class BundleExtracter(object):
    def __init__(self, config, base_path):
        self.base_path = base_path
        self.bundles_dir = config['bundles.bundles_dir']
        self.bundles_exts = config['bundles.bundles_exts']

    def abspath(self, bundle_path):
        return os.path.abspath(os.path.join(self.base_path, bundle_path))

    def is_bundle(self, path):
        abspath = self.abspath(path)
        if os.path.isfile(abspath):
            ext = os.path.splitext(path)[1][1:]
            return common_ancestor(path, self.bundles_dir) != '' and ext in self.bundles_exts
        return False

    def extract_bundle(self, bundle_path):
        if not self.is_bundle(bundle_path):
           raise RuntimeError('{} is not a recognized bundle.'.format(bundle_path))
        extracter = self._get_extracter(bundle_path)
        abspath = self.abspath(bundle_path)
        success, paths = extracter(abspath, self.base_path)
        return success, paths

    def _get_extracter(self, bundle_path):
        #TODO: Detect the extracter to be used based on the path
        return extract_zip_bundle

