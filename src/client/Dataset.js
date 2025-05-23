import Button from 'antd/es/button'
import styles from './Dataset.module.css'
import Select from 'antd/es/select'
import { useDispatch, useSelector } from 'react-redux'
import Query from './Query'
import File from './File'
import { createQuery } from './actions/query'
import { createFile } from './actions/file'
import Dropdown from 'antd/es/dropdown'
import { ConsoleSqlOutlined, UploadOutlined, MoreOutlined } from '@ant-design/icons'
import { Datasource } from './Datasource'
import { updateDatasetConnection } from './actions/dataset'
import { useHistory } from 'react-router-dom/cjs/react-router-dom'

function DatasetTypeSelector ({ dataset }) {
  const dispatch = useDispatch()

  const userDefinedConnection = useSelector(state => state.connection.userDefined)
  const connectionList = useSelector(state => state.connection.list)

  const env = useSelector(state => state.env)
  const { ALLOW_FILE_UPLOAD } = env.variables
  let allowFileUpload = ALLOW_FILE_UPLOAD
  let disabledFileUploadNote = 'File upload is disabled in Dekart configuration'
  if (allowFileUpload && userDefinedConnection) {
    // check if selected connection supports file upload
    const connection = connectionList.find(c => c.id === dataset.connectionId)
    allowFileUpload = connection?.canStoreFiles
    if (!allowFileUpload) {
      disabledFileUploadNote = 'Selected connection does not support file upload'
    }
  }

  return (
    <div className={styles.datasetTypeSelector}>
      <Dropdown
        disabled={!dataset.connectionId && userDefinedConnection}
        menu={{
          items: [
            {
              label: 'SQL query',
              icon: <ConsoleSqlOutlined />,
              key: 'sql'
            },
            {
              label: 'File upload',
              icon: <UploadOutlined />,
              title: !allowFileUpload ? disabledFileUploadNote : null,
              disabled: !allowFileUpload,
              key: 'file'
            }
          ],
          onClick: ({ key }) => {
            if (key === 'sql') {
              dispatch(createQuery(dataset.id))
            } else if (key === 'file') {
              dispatch(createFile(dataset.id))
            }
          }
        }}
      >
        <Button block type='primary'>Add data from...</Button>
      </Dropdown>
    </div>
  )
}

function DatasetSelector ({ dataset }) {
  const dispatch = useDispatch()
  const env = useSelector(state => state.env)
  const userDefinedConnection = useSelector(state => state.connection.userDefined)
  const isPlayground = useSelector(state => state.user.isPlayground)
  const isDefaultWorkspace = useSelector(state => state.user.isDefaultWorkspace)
  const connectionList = useSelector(state => state.connection.list)
  const selectedConnection = connectionList.find(c => c.id === dataset.connectionId)
  const history = useHistory()
  if (!env.loaded) {
    // do not render until environment is loaded
    return null
  }
  if (isPlayground && !isDefaultWorkspace) {
    // do not render in playground mode, but render in default workspace
    return null
  }
  const { ALLOW_FILE_UPLOAD } = env.variables
  if (!ALLOW_FILE_UPLOAD && !userDefinedConnection) {
    return null
  }
  return (
    <div className={styles.datasetSelector}>
      <div className={styles.selector}>
        {userDefinedConnection
          ? (
            <>
              <Datasource connection={selectedConnection} />
              <div className={styles.datasource}>
                <Select
                  placeholder='Select connection'
                  id='dekart-connection-select'
                  className={styles.connectionSelect}
                  value={dataset.connectionId || null}
                  onSelect={value => {
                    dispatch(updateDatasetConnection(dataset.id, value))
                  }}
                  options={[
                    ...(connectionList.map(connection => ({
                      value: connection.id,
                      label: connection.connectionName
                    })))
                  ]}
                /><Button
                  type='text'
                  title='Edit connections'
                  className={styles.connectionEditButton} icon={<MoreOutlined />}
                  onClick={() => {
                    history.push('/connections')
                  }}
                  />
              </div>
            </>
            )
          : <DatasetTypeSelector dataset={dataset} />}
      </div>
      {
        userDefinedConnection
          ? (
            <div className={styles.status}>
              <DatasetTypeSelector dataset={dataset} />
            </div>

            )
          : null
      }
    </div>
  )
}

export default function Dataset ({ dataset }) {
  let query = null
  let file = null
  const queries = useSelector(state => state.queries)
  const files = useSelector(state => state.files)
  if (dataset.queryId) {
    query = queries.find(q => q.id === dataset.queryId)
  } else if (dataset.fileId) {
    file = files.find(f => f.id === dataset.fileId)
  }
  return (
    <>
      {query ? <Query query={query} /> : file ? <File file={file} /> : <DatasetSelector dataset={dataset} />}
    </>
  )
}
