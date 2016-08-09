class DeploymentConfig(object):
    """

    """
    # This is very dynamic right now
    # This gives me enough getters and setters for now
    # TODO: have a schema of sorts.
    def __init(self, **kwargs):
        self.__dict__.update(kwargs)
