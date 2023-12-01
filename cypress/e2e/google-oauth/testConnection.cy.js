/* eslint-disable no-undef */
import copy from '../../fixtures/copy.json'

describe('basic query flow', () => {
  it('should make simple bigquery query and get ready status', () => {
    cy.visit('/')
    cy.get(`button:contains("${copy.create_report}")`).click()

    // create connection
    cy.get('#dekart-connection-select').click()
    cy.get('div.ant-select-item-option-content:contains("New")').click()
    const randomConnectionName = `test-${Math.floor(Math.random() * 1000000)}`
    cy.get('div.ant-modal-title').should('contain', 'Edit Connection')
    cy.get('input#connectionName').clear()
    cy.get('input#connectionName').type(randomConnectionName)
    cy.get('input#bigqueryProjectId').type('dekart-dev')
    cy.get('input#cloudStorageBucket').type('dekart-dev')
    cy.get('button:contains("Test Connection")').click()
    cy.get('button#saveConnection').should('be.enabled')
    cy.get('button#saveConnection').click()
  })
})
