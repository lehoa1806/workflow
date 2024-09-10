from typing import Dict, Iterable, Iterator, List

from .common import Start, Stop


class BaseStage:
    """
    A base class for pipeline stages in a data processing workflow. This class
    handles common functionalities such as managing input and output columns,
    as well as defining the setup, process, teardown, and run methods that are
    essential for data processing in stages.
    """

    def __init__(
        self,
        name: str = None,
    ) -> None:
        """
        Initializes the BaseStage with an optional name. If no name is
        provided, the class name is used as the default name.

        :param name: Optional; the name of the stage.
        """
        self.name = name or self.__class__.__name__

    @property
    def input_columns(self) -> List[str]:
        """
        Defines the list of input columns required by this stage.

        :return: List of input column names.
        """
        return []

    @property
    def output_columns(self) -> List[str]:
        """
        Defines the list of output columns produced by this stage.

        :return: List of output column names.
        """
        return []

    def get_input_item(self, data: Dict):
        """
        Retrieves the input item based on defined input columns from the
        given data.

        :param data: Input data dictionary.
        :return: Filtered data dictionary based on input columns or original
                 data if no input columns are defined.
        """
        if isinstance(data, Start) or isinstance(data, Stop):
            return data
        if self.input_columns:
            return {k: data[k] for k in self.input_columns}
        else:
            return data

    def get_output_item(self, data: Dict, **kwargs):
        """
        Produces the output item based on defined output columns and
        additional logged data.

        :param data: Input data dictionary to be processed.
        :param kwargs: Additional keyword arguments, including logged data.
        :return: Processed output data with logged information appended.
        """
        if isinstance(data, Start) or isinstance(data, Stop):
            return data
        logged_data = kwargs.get('logged_data') or {}
        if self.output_columns:
            out_item = {k: data[k] for k in self.output_columns}
        else:
            out_item = data
        return {**out_item, **logged_data}

    def setup(self, item: Dict) -> Iterator:
        """
        Setup method to initialize any resources or configurations needed
        before processing starts.

        :param item: The initial item or configuration passed to the setup.
        :return: An iterator over the processed items.
        """
        raise NotImplementedError

    def process(self, item: Dict) -> Iterator:
        """
        The core processing method for an item in the stage.

        :param item: The item to be processed.
        :return: An iterator over the processed items.
        """
        raise NotImplementedError

    def teardown(self, item: Dict) -> Iterator:
        """
        Teardown method to clean up resources or configurations after
        processing has finished.

        :param item: The final item or configuration passed to the teardown.
        :return: An iterator over the processed items.
        """
        raise NotImplementedError

    def run(self, source: Iterable = None, **kwargs) -> Iterator:
        """
        Runs the stage processing on a sequence of input items, applying the
        setup, process, and teardown methods.

        :param source: Iterable source of input data items.
        :param kwargs: Additional keyword arguments that may influence the
                       processing.
        :return: An iterator over all processed data items.
        """
        raise NotImplementedError
