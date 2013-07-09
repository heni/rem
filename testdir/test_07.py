import logging
import unittest
import rem


class T07(unittest.TestCase):
    """Checking internal REM structures"""

    def testTagWrapperSerialization(self):
        import cPickle

        tag = rem.Tag("test")
        wrapOrig = rem.storages.TagWrapper(tag)
        wrapDesc = cPickle.dumps(wrapOrig)
        wrapNew = cPickle.loads(wrapDesc)
        self.assertTrue(isinstance(wrapNew, rem.storages.TagWrapper))
        self.assertEqual(wrapNew.name, wrapOrig.name)

