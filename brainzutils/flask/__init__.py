from flask import Flask
from flask_uuid import FlaskUUID
from flask_debugtoolbar import DebugToolbarExtension
from brainzutils.flask import loggers


class CustomFlask(Flask):
    """Custom version of Flask with our bells and whistles."""

    def __init__(self, import_name, config_file=None, debug=None,
                 use_flask_uuid=False,
                 *args, **kwargs):
        """Create an instance of Flask app.

            See original documentation for Flask.

            Arguments:
                import_name (str): Name of the application package.
                config_file (str): Path to a config file that needs to be loaded.
                    Should be in a form of Python module.
                debug (bool): Override debug value.
                use_flask_uuid (bool): Turn on Flask-UUID extension if set to True.
        """
        super(CustomFlask, self).__init__(import_name, *args, **kwargs)
        if config_file:
            self.config.from_pyfile(config_file)
        if debug is not None:
            self.debug = debug
        if use_flask_uuid:
            FlaskUUID(self)


    def init_debug_toolbar(self):
        """This method initializes the Flask-Debug extension toolbar for the
        Flask app.

        Note that the Flask-Debug extension requires app.debug be true
        and the SECRET_KEY be defined in app.config.
        """
        if self.debug:
            DebugToolbarExtension(self)


    def init_loggers(self,
                     file_config=None,
                     email_config=None,
                     sentry_config=None):
        """This method attaches loggers to the Flask app.

        Each type of logger has its own configuration which needs to be passed
        as an argument to this method. If you don't want to enable one of
        available loggers, set its configuration to None.

        Logging levels (when specified) need to be one of supported by
        `logging` module, as an integer or a valid string.

        Args:
            file_config (dict): Dictionary with the following structure:
                {
                    'filename': 'log.txt',
                    'max_bytes': 512 * 1024,  # optional
                    'backup_count': 100,      # optional
                 }
            email_config (dict): Dictionary with the following structure:
                {
                    'mail_server': 'localhost',
                    'mail_port': 25,
                    'mail_from_host': 'example.org',
                    'log_email_recipients': [
                        'user1@example.org',
                        'user2@example.org',
                    ],
                    'log_email_topic': 'Error occurred',
                    'level': 'ERROR',  # optional
                 }
            sentry_config (dict): Dictionary with the following structure:
                {
                    'dsn': 'YOUR_SENTRY_DSN',
                    'level': 'WARNING',  # optional
                 }
        """
        if file_config:
            loggers.add_file_handler(self, **file_config)
        if email_config:
            loggers.add_email_handler(self, **email_config)
        if sentry_config:
            loggers.add_sentry(self, **sentry_config)
