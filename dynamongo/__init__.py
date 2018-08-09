# -*- coding: utf-8 -*-

from .connection import Connection
from .models import BaseModel, GlobalIndex, LocalIndex, IMeta
from .conditions import *
from .updates import *
from .exceptions import *
from .utils import *
from .attributes import *

# todo: add support for condition inversion

__version__ = '0.2'
__author__ = 'Musyoka Morris'
