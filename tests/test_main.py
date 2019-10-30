""" Tests of karr_lab_aws_manager command line interface (karr_lab_aws_manager.__main__)

:Author: Name <email>
:Date: 2019-9-16
:Copyright: 2019, Karr Lab
:License: MIT
"""

from karr_lab_aws_manager import __main__
import karr_lab_aws_manager
import capturer
import mock
import unittest


class CliTestCase(unittest.TestCase):

    def test_cli(self):
        with mock.patch('sys.argv', ['karr_lab_aws_manager', '--help']):
            with self.assertRaises(SystemExit) as context:
                __main__.main()
                self.assertRegex(context.Exception, 'usage: karr_lab_aws_manager')

    def test_help(self):
        with self.assertRaises(SystemExit):
            with __main__.App(argv=['--help']) as app:
                app.run()

    def test_version(self):
        with __main__.App(argv=['-v']) as app:
            with capturer.CaptureOutput(merged=False, relay=False) as captured:
                with self.assertRaises(SystemExit):
                    app.run()
                self.assertEqual(captured.stdout.get_text(), karr_lab_aws_manager.__version__)
                self.assertEqual(captured.stderr.get_text(), '')

        with __main__.App(argv=['--version']) as app:
            with capturer.CaptureOutput(merged=False, relay=False) as captured:
                with self.assertRaises(SystemExit):
                    app.run()
                self.assertEqual(captured.stdout.get_text(), karr_lab_aws_manager.__version__)
                self.assertEqual(captured.stderr.get_text(), '')

    def test_command_1(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['cmd1']) as app:
                # run app
                app.run()

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), 'command_1 output')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_command_2(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['cmd2']) as app:
                # run app
                app.run()

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), 'command_2 output')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_command_3(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['command-3',
                                    'arg-1 value',
                                    'arg-2 value',
                                    '--opt-arg-3', 'opt-arg-3 value',
                                    '--opt-arg-4', '3.14']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertTrue(app.pargs.arg_1)
                self.assertTrue(app.pargs.arg_2)
                self.assertTrue(app.pargs.opt_arg_3)
                self.assertTrue(app.pargs.opt_arg_4)

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_es_bulk_upload(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['es-bulk-upload',
                                    'cursor_value',
                                    'id_value']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertTrue(app.pargs.cursor)
                self.assertTrue(app.pargs.id)

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_es_set_idx(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['es-set-idx',
                                    'test',
                                    '1']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.index, 'test')
                self.assertTrue(app.pargs.replica_count)

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_es_del_idx(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['es-del-idx',
                                    'test',
                                    '--id', 'test_id']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.index, 'test')
                self.assertEqual(app.pargs.id, 'test_id')

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_quilt_add2_package(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['quilt-add2-package',
                                    'test/quilt_cl',
                                    'test_destination',
                                    'requirements.txt',
                                    '--meta', "{'test': 'test_meta'}"]) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.destination, 'test_destination')
                self.assertEqual(app.pargs.source, 'requirements.txt')
                self.assertEqual(app.pargs.default_remote_registry, 's3://karrlab')

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertTrue('Hashing:' in captured.stderr.get_text())

    def test_quilt_push2_s3(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['quilt-push2-s3',
                                    'test_name',
                                    '--message', 'some message']) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.package_name, 'test_name')
                self.assertEqual(app.pargs.destination, 's3://karrlab')

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')

    def test_quilt_rm_pkg(self):
        with capturer.CaptureOutput(merged=False, relay=False) as captured:
            with __main__.App(argv=['quilt-rm-pkg',
                                    'test_user/test_name',
                                    '--delete_from_s3', False]) as app:
                # run app
                app.run()

                # test that the arguments to the CLI were correctly parsed
                self.assertEqual(app.pargs.package_name, 'test_user/test_name')
                self.assertEqual(app.pargs.delete_from_s3, False)

                # test that the CLI produced the correct output
                self.assertEqual(captured.stdout.get_text(), '')
                self.assertEqual(captured.stderr.get_text(), '')