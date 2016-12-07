from zc_events.request import RemoteResourceWrapper, RemoteResourceListWrapper


class TestResourceWrapper:
    def test_remote_resource_wrapper(self):
        data = {
            "data": {
                "type": "articles",
                "id": "1",
                "attributes": {
                    "title": "Omakase"
                }
            }
        }

        resource = RemoteResourceWrapper(data['data'])

        assert resource.title == 'Omakase'
        assert resource.type == 'articles'
        assert resource.id == '1'

    def test_remote_resource_wrapper_one_relationship(self):
        data = {
            "data": {
                "type": "articles",
                "id": "1",
                "attributes": {
                    "title": "Omakase"
                },
                "relationships": {
                    "author": {
                        "links": {
                            "self": "/articles/1/relationships/author",
                            "related": "/articles/1/author"
                        },
                        "data": {"type": "People", "id": "9"}
                    }
                }
            }
        }

        resource = RemoteResourceWrapper(data['data'])

        assert resource.author.type == 'People'
        assert resource.author.id == '9'

    def test_remote_resource_wrapper_multiple_relationship(self):
        data = {
            "data": {
                "type": "articles",
                "id": "1",
                "attributes": {
                    "title": "Omakase"
                },
                "relationships": {
                    "authors": {
                        "links": {
                            "self": "/articles/1/relationships/authors",
                            "related": "/articles/1/authors"
                        },
                        "data": [
                            {"type": "People", "id": "9"},
                        ]
                    }
                }
            }
        }

        resource = RemoteResourceWrapper(data['data'])

        assert isinstance(resource.authors, list)
        assert resource.authors[0].type == 'People'
        assert resource.authors[0].id == '9'


class TestRemoteResourceListWrapper:
    def test_remote_resource_list_wrapper(self):
        data = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {
                        "title": "Omakase"
                    }
                }
            ]
        }

        resources = RemoteResourceListWrapper(data['data'])

        assert isinstance(resources, list)
        assert resources[0].title == 'Omakase'
        assert resources[0].type == 'articles'
        assert resources[0].id == '1'

    def test_remote_resource_list_wrapper_one_relationship(self):
        data = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {
                        "title": "Omakase"
                    },
                    "relationships": {
                        "author": {
                            "links": {
                                "self": "/articles/1/relationships/author",
                                "related": "/articles/1/author"
                            },
                            "data": {"type": "People", "id": "9"}
                        }
                    }
                }
            ]
        }

        resources = RemoteResourceListWrapper(data['data'])

        assert resources[0].author.type == 'People'
        assert resources[0].author.id == '9'

    def test_remote_resource_list_wrapper_multiple_relationship(self):
        data = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {
                        "title": "Omakase"
                    },
                    "relationships": {
                        "authors": {
                            "links": {
                                "self": "/articles/1/relationships/authors",
                                "related": "/articles/1/authors"
                            },
                            "data": [
                                {"type": "People", "id": "9"},
                            ]
                        }
                    }
                }
            ]
        }

        resources = RemoteResourceListWrapper(data['data'])

        assert resources[0].authors[0].type == 'People'
        assert resources[0].authors[0].id == '9'

    def test_remote_resource_list_wrapper_multiple_relationship_links(self):
        data = {
            "data": [
                {
                    "type": "articles",
                    "id": "1",
                    "attributes": {
                        "title": "Omakase"
                    },
                    "relationships": {
                        "authors": {
                            "links": {
                                "self": "/articles/1/relationships/authors",
                                "related": "/articles/1/authors"
                            },
                            "data": [
                                {"type": "People", "id": "9"},
                            ]
                        }
                    }
                }
            ]
        }

        resources = RemoteResourceListWrapper(data['data'])

        assert resources[0].authors.links.self == '/articles/1/relationships/authors'
        assert resources[0].authors.links.related == '/articles/1/authors'
