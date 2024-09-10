class Error(dict):
    """
    A custom error object that extends the dictionary type, allowing for errors
    to be recorded and passed alongside regular data within a data processing
    pipeline. This can include error messages and additional metadata related
    to the error condition.
    """
    def __init__(self, msg: str = '', **kwargs) -> None:
        """
        Initializes the Error object with an optional error message and any
        additional keyword arguments to be stored in the dictionary.

        :param msg: An optional error message describing the error condition.
        :param kwargs: Optional keyword arguments representing additional
                       metadata about the error. These are stored as key-value
                       pairs within the object.
        """
        super().__init__(**kwargs)
        self.update({'message': msg})


class Start(dict):
    """
    A signaling object that indicates the start of a data stream or
    processing pipeline. It is often used to signify the initialization
    phase of data processing, allowing for any setup operations that need to
    occur before the main data processing begins.
    """
    def __init__(self, **kwargs) -> None:
        """
        Initializes the Start signal with any additional metadata as keyword
        arguments.

        :param kwargs: Optional keyword arguments for carrying additional
                       metadata.
        """
        super().__init__(**kwargs)


class Stop(dict):
    """
    A signaling object that indicates the end of a data stream or processing
    pipeline. It is used to signify the conclusion of data processing,
    allowing for any teardown or finalization operations that need to occur
    after the main data processing has completed.
    """
    def __init__(self, **kwargs) -> None:
        """
        Initializes the Stop signal with any additional metadata as keyword
        arguments.

        :param kwargs: Optional keyword arguments for carrying additional
                       metadata.
        """
        super().__init__(**kwargs)
