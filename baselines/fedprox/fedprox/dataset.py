"""MNIST dataset utilities for federated learning."""

from typing import Optional, Tuple

import torch
from omegaconf import DictConfig
from torch.utils.data import DataLoader, random_split

from fedprox.dataset_preparation import _partition_data
from flwr_datasets import FederatedDataset
from torchvision.transforms import Compose, Normalize, ToTensor


def load_datasets(  # pylint: disable=too-many-arguments
    config: DictConfig,
    num_clients: int,
    val_ratio: float = 0.1,
    batch_size: Optional[int] = 32,
    partition_id: Optional[int] = 0,
    seed: Optional[int] = 42,
) -> Tuple[DataLoader, DataLoader, DataLoader]:
    """Create the dataloaders to be fed into the model.

    Parameters
    ----------
    config: DictConfig
        Parameterises the dataset partitioning process
    num_clients : int
        The number of clients that hold a part of the data
    val_ratio : float, optional
        The ratio of training data that will be used for validation (between 0 and 1),
        by default 0.1
    batch_size : int, optional
        The size of the batches to be fed into the model, by default 32
    seed : int, optional
        Used to set a fix seed to replicate experiments, by default 42

    Returns
    -------
    Tuple[DataLoader, DataLoader, DataLoader]
        The DataLoader for training, the DataLoader for validation, the DataLoader
        for testing.
    """
    print(f"Dataset partitioning config: {config}")
    # datasets, testset = _partition_data(
    #     num_clients,
    #     iid=config.iid,
    #     balance=config.balance,
    #     power_law=config.power_law,
    #     seed=seed,
    # )
    # # Split each partition into train/val and create DataLoader
    # trainloaders = []
    # valloaders = []
    # for dataset in datasets:
    #     len_val = int(len(dataset) / (1 / val_ratio))
    #     lengths = [len(dataset) - len_val, len_val]
    #     ds_train, ds_val = random_split(
    #         dataset, lengths, torch.Generator().manual_seed(seed)
    #     )
    #     trainloaders.append(DataLoader(ds_train, batch_size=batch_size, shuffle=True))
    #     valloaders.append(DataLoader(ds_val, batch_size=batch_size))
    # return trainloaders, valloaders, DataLoader(testset, batch_size=batch_size)

    mnist_fds = FederatedDataset(dataset="mnist", partitioners={"train": num_clients})
    partition = mnist_fds.load_partition(partition_id)
    testset = mnist_fds.load_split("test")

    # Divide data on each node: 80% train, 20% test
    partition_train_test = partition.train_test_split(test_size=val_ratio, seed=seed)
    pytorch_transforms = Compose([ToTensor(), Normalize((0.5,), (0.5,))])

    def apply_transforms(batch):
        """Apply transforms to the partition from FederatedDataset."""
        batch["image"] = [pytorch_transforms(img) for img in batch["image"]]
        return batch

    partition_train_test = partition_train_test.with_transform(apply_transforms)
    trainloader = DataLoader(partition_train_test["train"], batch_size=32, shuffle=True)
    valloader = DataLoader(partition_train_test["test"], batch_size=32)
    testloader = DataLoader(testset, batch_size=batch_size)
    return trainloader, valloader, testloader