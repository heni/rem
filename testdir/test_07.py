import unittest
import rem
import six
from six.moves import cPickle as pickle

class T07(unittest.TestCase):
    """Checking internal REM structures"""

    def testTagWrapperSerialization(self):
        import pickle

        tag = rem.Tag("test")
        wrapOrig = rem.storages.TagWrapper(tag)
        wrapDesc = pickle.dumps(wrapOrig)
        wrapNew = pickle.loads(wrapDesc)
        self.assertTrue(isinstance(wrapNew, rem.storages.TagWrapper))
        self.assertEqual(wrapNew.name, wrapOrig.name)

